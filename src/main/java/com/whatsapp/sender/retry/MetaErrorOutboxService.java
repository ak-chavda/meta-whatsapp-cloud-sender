package com.whatsapp.sender.retry;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dao.MetaErrorOutboxDocument;

/**
 * Service to handle the write-path (batch inserts) and read-path (row-level locking)
 * for the Transactional Outbox Pattern.
 * <p>
 * Supports three categories of retryable errors:
 * <ul>
 *   <li><strong>130429 (Burst/MPS Limit)</strong>: Exponential backoff (5s→60s cap)</li>
 *   <li><strong>80007 (Daily Quota Limit)</strong>: Strict 24-hour retry</li>
 *   <li><strong>5xx (Server Errors)</strong>: Short exponential backoff (60s, 120s, 300s cap)</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MetaErrorOutboxService {

    private final MongoTemplate mongoTemplate;

    // In-memory buffer to prevent Thundering Herd on the database
    private final ConcurrentLinkedQueue<MetaErrorOutboxDocument> errorBuffer = new ConcurrentLinkedQueue<>();

    private static final int BATCH_INSERT_SIZE = 1000;

    public void queueForRetry(
            Integer campaignId,
            Integer batchId,
            String wabaId,
            String wabaPhoneNumberId,
            String templateId,
            List<String> targetPhoneNumbers,
            String errorCode,
            String errorMessage,
            int retryCount) {

        Instant retryAfter = calculateRetryAfter(errorCode, retryCount);
        MetaErrorOutboxDocument doc = new MetaErrorOutboxDocument(
                UUID.randomUUID().toString(),
                campaignId,
                batchId,
                wabaId,
                wabaPhoneNumberId,
                templateId,
                targetPhoneNumbers,
                errorCode,
                errorMessage,
                "PENDING",
                retryCount,
                retryAfter,
                Instant.now(),
                null // workerId
        );

        errorBuffer.add(doc);
        log.debug("Queued {} targets for retry. Error: {}, RetryAfter: {}, RetryCount: {}", targetPhoneNumbers.size(), errorCode, retryAfter, retryCount);
    }

    /**
     * Calculates the retry-after timestamp based on error type and retry count.
     * <p>
     * <ul>
     *   <li><strong>80007</strong>: Strict 24 hours (daily quota is a hard stop)</li>
     *   <li><strong>130429</strong>: Exponential backoff (5s, 10s, 20s, 40s, 60s cap)</li>
     *   <li><strong>5xx / HTTP_5xx</strong>: Exponential backoff (60s, 120s, 300s cap)</li>
     *   <li><strong>Fallback</strong>: 60 seconds</li>
     * </ul>
     */
    private Instant calculateRetryAfter(String errorCode, int retryCount) {
        if ("80007".equals(errorCode)) {
            // Daily Quota Limit: Hard stop. Strict 24 hours.
            return Instant.now().plus(24, ChronoUnit.HOURS);
        }

        if ("130429".equals(errorCode)) {
            // Burst Limit: Exponential backoff (5s, 10s, 20s, 40s, 60s cap)
            int baseDelaySeconds = 5 * (int) Math.pow(2, retryCount);
            if (baseDelaySeconds > 60) {
                baseDelaySeconds = 60;
            }
            return Instant.now().plus(baseDelaySeconds, ChronoUnit.SECONDS);
        }

        if (errorCode != null && errorCode.startsWith("HTTP_5")) {
            // 5xx Server Error: Exponential backoff (60s, 120s, 300s cap)
            int baseDelaySeconds = 60 * (int) Math.pow(2, retryCount);
            if (baseDelaySeconds > 300) {
                baseDelaySeconds = 300;
            }
            return Instant.now().plus(baseDelaySeconds, ChronoUnit.SECONDS);
        }

        // Fallback: 60 seconds
        return Instant.now().plus(60, ChronoUnit.SECONDS);
    }

    /**
     * Scheduled task to drain the in-memory buffer and batch insert into MongoDB.
     * Runs frequently to ensure low latency without overwhelming the DB.
     */
    @Scheduled(fixedDelay = 1000)
    public void flushBufferToDatabase() {
        if (errorBuffer.isEmpty()) {
            return;
        }

        List<MetaErrorOutboxDocument> batch = new ArrayList<>();
        while (!errorBuffer.isEmpty() && batch.size() < BATCH_INSERT_SIZE) {
            MetaErrorOutboxDocument doc = errorBuffer.poll();
            if (doc != null) {
                batch.add(doc);
            }
        }

        if (!batch.isEmpty()) {
            try {
                mongoTemplate.insertAll(batch);
                log.debug("Batch inserted {} failed messages to meta_error_outbox", batch.size());
            } catch (Exception e) {
                log.error("Failed to batch insert to meta_error_outbox: {}", e.getMessage(), e);
                // Re-queue the failed documents to avoid data loss
                errorBuffer.addAll(batch);
            }
        }
    }

    /**
     * Atomically locks ripe messages for a given error code pattern and returns them.
     * Uses atomic updateMany to assign a unique worker ID, then queries by that ID.
     *
     * @param errorCodePattern exact match for error_code field (e.g., "130429", "80007") OR a regex prefix for 5xx errors
     * @param is5xx            if true, matches any error_code starting with "HTTP_5"
     * @param limit            max documents to lock
     */
    public List<MetaErrorOutboxDocument> fetchAndLockRipeMessages(String errorCodePattern, boolean is5xx, int limit) {
        String workerId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        Criteria criteria = Criteria.where("status").is("PENDING").and("retry_after").lte(now);

        if (is5xx) {
            criteria = criteria.and("error_code").regex("^HTTP_5");
        } else {
            criteria = criteria.and("error_code").is(errorCodePattern);
        }

        Query lockQuery = new Query(criteria).limit(limit);
        Update lockUpdate = new Update().set("status", "PROCESSING").set("worker_id", workerId);

        // Perform atomic updateMany (row-level locking)
        mongoTemplate.updateMulti(lockQuery, lockUpdate, MetaErrorOutboxDocument.class);

        // Fetch the locked documents
        Query fetchQuery = new Query(Criteria.where("worker_id").is(workerId));
        return mongoTemplate.find(fetchQuery, MetaErrorOutboxDocument.class);
    }

    /**
     * Removes the document from the active outbox after it has been
     * successfully pushed to the Kafka retry topic.
     */
    public void deleteFromOutbox(MetaErrorOutboxDocument doc) {
        Query deleteQuery = new Query(Criteria.where("id").is(doc.id()));
        mongoTemplate.remove(deleteQuery, MetaErrorOutboxDocument.class);
    }
}