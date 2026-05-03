package com.whatsapp.sender.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dao.MetaErrorOutboxDocument;
import com.whatsapp.sender.dto.FailureEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Scheduled tasks for the Database Polling / Transactional Outbox Pattern.
 * <p>
 * Three schedulers poll MongoDB for ripe retryable messages and push them
 * to the {@code whatsapp-failures-retry} Kafka topic:
 * <ul>
 *   <li><strong>Fast (130429 — Burst/MPS Limit)</strong>: Every 5 seconds.
 *       Scope: per Phone Number ID.</li>
 *   <li><strong>Slow (80007 — Daily Quota)</strong>: Every 15 minutes.
 *       Scope: per WABA (all phone numbers share the pool).</li>
 *   <li><strong>Medium (5xx — Server Errors)</strong>: Every 30 seconds.
 *       Transient server errors with exponential backoff.</li>
 * </ul>
 * <p>
 * After successfully pushing to the retry topic, the document is <strong>deleted</strong>
 * from the outbox to keep the collection lean. The retry topic is the handoff point.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetaErrorSchedulers {

    private final MetaErrorOutboxService outboxService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.whatsapp-failures-retry:whatsapp-failures-retry}")
    private String retryTopic;

    private static final int POLL_LIMIT = 500;

    /**
     * Fast Scheduler: Runs every 5 seconds.
     * Queries the DB for ripe messages where error_code = 130429 (Burst Limit).
     * Scope: per Phone Number ID.
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void processBurstLimitRetryOutbox() {
        processRipeMessages("130429", false);
    }

    /**
     * Slow Scheduler: Runs every 15 minutes.
     * Queries the DB for ripe messages where error_code = 80007 (Daily Quota Limit).
     * Scope: per WABA (all phone numbers share the quota pool).
     */
    @Scheduled(fixedDelay = 900000, initialDelay = 10000) // 15 mins = 900000 ms
    public void processDailyQuotaRetryOutbox() {
        processRipeMessages("80007", false);
    }

    /**
     * Medium Scheduler: Runs every 30 seconds.
     * Queries the DB for ripe messages where error_code starts with "HTTP_5" (5xx server errors).
     */
    @Scheduled(fixedDelay = 30000, initialDelay = 15000)
    public void processServerErrorRetryOutbox() {
        processRipeMessages("HTTP_5", true);
    }

    /**
     * Common logic to fetch ripe messages with row-level locking, push to Kafka retry topic,
     * and delete them from the outbox.
     *
     * @param errorCodePattern the error code to match (exact or prefix)
     * @param is5xx            if true, uses regex matching for HTTP_5xx codes
     */
    private void processRipeMessages(String errorCodePattern, boolean is5xx) {
        try {
            // 1. Fetch & Lock (Atomic)
            List<MetaErrorOutboxDocument> ripeMessages = outboxService.fetchAndLockRipeMessages(errorCodePattern, is5xx, POLL_LIMIT);
            
            if (ripeMessages == null || ripeMessages.isEmpty()) {
                return;
            }

            log.info("Found {} ripe messages for error pattern [{}]", ripeMessages.size(), errorCodePattern);

            // 2. Process each locked document
            for (MetaErrorOutboxDocument doc : ripeMessages) {
                try {
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

                    String payload = objectMapper.writeValueAsString(failureEvent);

                    // 3. Push payload to retry-topic, then delete from outbox
                    kafkaTemplate.send(retryTopic, String.valueOf(doc.campaignId()), payload)
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    log.error("Failed to push doc [{}] to retry topic: {}", doc.id(), ex.getMessage());
                                    outboxService.markAsFailed(doc);
                                } else {
                                    log.debug("Successfully pushed doc [{}] to retry topic. Deleting from outbox.", doc.id());
                                    outboxService.deleteFromOutbox(doc);
                                }
                            });

                } catch (Exception e) {
                    log.error("Error pushing outbox doc [{}] to Kafka: {}", doc.id(), e.getMessage(), e);
                    outboxService.markAsFailed(doc);
                }
            }

        } catch (Exception e) {
            log.error("Exception in MetaErrorScheduler for pattern [{}]: {}", errorCodePattern, e.getMessage(), e);
        }
    }
}
