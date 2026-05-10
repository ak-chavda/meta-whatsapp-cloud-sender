package com.whatsapp.sender.retry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dao.MetaErrorOutboxDocument;
import com.whatsapp.sender.dto.FailureEvent;

/**
 * Scheduled tasks for the Database Polling / Transactional Outbox Pattern.
 * Three schedulers poll MongoDB for ripe retryable messages and push them to the retryable events Kafka topic:
 * <ul>
 *   <li><strong>Fast (130429 — Burst/MPS Limit)</strong>: Every 5 seconds. Scope: per Phone Number ID.</li>
 *   <li><strong>Slow (80007 — Daily Quota)</strong>: Every 15 minutes. Scope: per WABA (all phone numbers share the pool).</li>
 *   <li><strong>Medium (5xx — Server Errors)</strong>: Every 30 seconds. Transient server errors with exponential backoff.</li>
 * </ul>
 * <p>
 * After successfully pushing to the retry topic, the document is <strong>deleted</strong>
 * from the outbox to keep the collection lean. The retry topic is the handoff point.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetaErrorSchedulers {

    ////////////////////////////////////////////////////////////
    // This file is looks fine only 
    // I am thinking to use the scheduler only not to push to kafka for retry topic instead directly pick from DB and process it and then save it and delete it from retry table entry.

    private final MetaErrorOutboxService outboxService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.campaign-failures-retry}")
    private String retryTopic;

    @Value("${app.delayed-retry.batch-size}")
    private int pollLimit;

    /**
     * Fast Scheduler: Runs every 10 seconds.
     * Queries the DB for ripe messages where error_code = 130429 (Burst Limit).
     * Scope: per WaBa-Phone-Number-ID.
     */
    @Scheduled(fixedDelay = 10_000, initialDelay = 5_000)
    public void processBurstLimitRetryOutbox() {
        processRipeMessages("130429", false);
    }

    /**
     * Slow Scheduler: Runs every 15 minutes. (9_00_000 ms)
     * Queries the DB for ripe messages where error_code = 80007 (Daily Quota Limit).
     * Scope: per WABA (all phone numbers share the quota pool).
     */
    @Scheduled(fixedDelay = 9_00_000, initialDelay = 10_000)
    public void processDailyQuotaRetryOutbox() {
        processRipeMessages("80007", false);
    }

    /**
     * Medium Scheduler: Runs every 45 seconds.
     * Queries the DB for ripe messages where error_code starts with "HTTP_5" (5xx server errors).
     */
    @Scheduled(fixedDelay = 45_000, initialDelay = 15_000)
    public void processServerErrorRetryOutbox() {
        processRipeMessages("HTTP_5", true);
    }

    /**
     * Common logic to fetch ripe messages with row-level locking, push to Kafka retry topic,
     * and delete them from the outbox.
     */
    private void processRipeMessages(String errorCodePattern, boolean is5xx) {
        try {
            // Fetch & Lock (Atomic)
            List<MetaErrorOutboxDocument> ripeMessages = outboxService.fetchAndLockRipeMessages(errorCodePattern, is5xx, pollLimit);
            if (ripeMessages == null || ripeMessages.isEmpty()) {
                log.info("Found 0 ripe messages for error pattern [{}]", errorCodePattern);
                return;
            }

            // Process each locked document
            log.info("Found {} ripe messages for error pattern [{}]", ripeMessages.size(), errorCodePattern);
            for (MetaErrorOutboxDocument doc : ripeMessages) {
                FailureEvent failureEvent = new FailureEvent(
                    doc.campaignId(),
                    doc.batchId(),
                    doc.wabaId(),
                    doc.wabaPhoneNumberId(),
                    doc.templateId(),
                    doc.targetPhoneNumbers(),
                    doc.errorCode(),
                    doc.errorMessage(),
                    doc.retryCount(),
                    Instant.now()
                );

                try {
                    String payload = objectMapper.writeValueAsString(failureEvent);

                    // Push payload to retry-topic, then delete from outbox
                    kafkaTemplate.send(retryTopic, String.valueOf(doc.campaignId()), payload)
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    log.error("Failed to push doc [{}] to retry topic: {}", doc.id(), ex.getMessage());
                                    // don't delete from outbox, let the scheduler retry
                                } else {
                                    log.debug("Successfully pushed doc [{}] to retry topic. Deleting from outbox.", doc.id());
                                    outboxService.deleteFromOutbox(doc);
                                }
                            });

                } catch (Exception e) {
                    log.error("Error pushing outbox doc [{}] to Kafka: {}", doc.id(), e.getMessage(), e);
                    // don't delete from outbox, let the scheduler retry
                }
            }

        } catch (Exception e) {
            log.error("Exception in MetaErrorScheduler for error-pattern [{}]: {}", errorCodePattern, e.getMessage(), e);
        }
    }
}