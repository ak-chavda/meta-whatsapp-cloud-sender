package com.whatsapp.sender.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.OutboundBatchEvent;
import com.whatsapp.sender.service.BatchDispatcher;
import com.whatsapp.sender.service.KillSwitchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for the {@code campaign-outbound-batch} topic.
 * <p>
 * This is the entry point of the Sender Service pipeline:
 * <pre>
 * Kafka Consume → Kill Switch Check → Virtual Thread Fan-out → MongoDB Persist
 * </pre>
 * <p>
 * Processing flow for each consumed record:
 * <ol>
 *   <li><strong>Kill Switch (Pre-flight)</strong>: Check Redis for campaign status.
 *       If PAUSED or CANCELLED, log and discard the batch.</li>
 *   <li><strong>Dispatch</strong>: Fan out HTTP calls to the WhatsApp Cloud API
 *       using Virtual Threads. Block until all targets are processed.</li>
 *   <li><strong>Acknowledge</strong>: Commit the Kafka offset only after all
 *       processing is complete.</li>
 * </ol>
 * <p>
 * Result routing:
 * <ul>
 *   <li><strong>Success</strong>: Persisted to {@code MessageDispatchDocument} (MongoDB).</li>
 *   <li><strong>Retryable failure</strong>: Queued to {@code MetaErrorOutboxDocument} (MongoDB).
 *       Schedulers pick these up and push to the retry Kafka topic.</li>
 *   <li><strong>Non-retryable failure</strong>: Persisted to {@code MessageDispatchDocument}
 *       (MongoDB) + DLQ.</li>
 * </ul>
 * <p>
 * The offset is committed LAST, ensuring at-least-once delivery semantics.
 * If the service crashes mid-batch, the batch will be re-consumed and reprocessed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboundBatchListener {

    private final KillSwitchService killSwitchService;
    private final BatchDispatcher batchDispatcher;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topics.campaign-outbound-batch}")
    public void onBatchReceived(@Payload String payload, Acknowledgment acknowledgment) {

        OutboundBatchEvent batch;
        try {
            batch = objectMapper.readValue(payload, OutboundBatchEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialize batch payload, acknowledging and skipping. Payload: {}, Exception: {}", payload, e.getMessage());
            e.printStackTrace();
            acknowledgment.acknowledge();
            return;
        }

        Integer batchId = batch.batchId();
        Integer campaignId = batch.campaignId();
        int targetCount = batch.targetPhoneNumbers() != null ? batch.targetPhoneNumbers().size() : 0;
        log.info("Received batch [{}] for campaign [{}], Targets: {}", batchId, campaignId, targetCount);

        try {
            // ── Step 1: Pre-flight Kill Switch ──────────────────────────────
            if (killSwitchService.shouldDiscardBatch(campaignId)) {
                log.info("Batch [{}] discarded by kill switch. Campaign [{}] is paused/cancelled.", batchId, campaignId);
                acknowledgment.acknowledge();
                return;
            }

            // ── Step 2: Validate batch payload ──────────────────────────────
            if (batch.targetPhoneNumbers() == null || batch.targetPhoneNumbers().isEmpty()) {
                log.warn("Batch [{}] for campaign [{}] has no targets. Acknowledging and skipping.", batchId, campaignId);
                acknowledgment.acknowledge();
                return;
            }

            // ── Step 3: Fan-out HTTP calls via Virtual Threads ──────────────
            batchDispatcher.dispatchBatch(batch);

            // ── Step 4: Commit offset (at-least-once guarantee) ─────────────
            acknowledgment.acknowledge();

            log.info("Batch [{}] fully processed and acknowledged. Campaign [{}], Targets: {}", batchId, campaignId, targetCount);

        } catch (Exception ex) {
            // Do NOT acknowledge — let Kafka redeliver this batch.
            log.error("Unhandled exception processing batch [{}] for campaign [{}]. Offset NOT committed — batch will be redelivered. Error: {}", batchId, campaignId, ex.getMessage(), ex);
        }
    }
}
