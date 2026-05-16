package com.whatsapp.sender.retry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dao.MessageDispatchDocument;
import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.OutboundBatchEvent;
import com.whatsapp.sender.enums.FailureStatus;
import com.whatsapp.sender.repository.MessageDispatchRepository;
import com.whatsapp.sender.service.BatchDispatcher;
import com.whatsapp.sender.service.CampaignService;
import com.whatsapp.sender.service.MessageStateService;
import com.whatsapp.sender.service.QuotaManager;
import com.whatsapp.sender.service.WhatsappApiClient;

/**
 * Retry Worker -- Kafka consumer for the rretryable events kafka topic.
 * Consumes retryable failure events that were pushed by the {@link RetrySchedulers}
 * from the MongoDB outbox after their retry-after time elapsed.
 * <p>
 * Processing flow:
 * <ol>
 *   <li><strong>Deserialize</strong> the {@link retryEvent} from the retry topic.</li>
 *   <li><strong>Check retry count</strong>: If max retries exhausted → save to {@code message_dispatch_log} as permanent failure + DLQ.</li>
 *   <li><strong>Fetch access token</strong> from the external Campaign Service.</li>
 *   <li><strong>Re-execute the WhatsApp API call</strong> for each target phone number using Virtual Threads.</li>
 *   <li><strong>On success</strong>: {@link QuotaManager#recordSuccessAndCheckLimits} handles quota increment + circuit check. Save to MongoDB.</li>
 *   <li><strong>On retryable failure</strong>: {@link QuotaManager#handleRetryableError} handles circuit opening. Queue back to outbox.</li>
 *   <li><strong>On non-retryable failure</strong>: Save to {@code MessageDispatchDocument} as permanent failure + DLQ.</li>
 * </ol>
 * All quota/circuit operations are delegated to {@link QuotaManager}. This class does NOT directly call CircuitBreaker.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryWorkerListener {

// ////////////////////////////////////////////////////////////
// This file is looks fine only 
// I am thinking to use the scheduler only, not to push to kafka for retry topic instead, 
// directly pick from DB and process it and then save it and delete it from retry table entry.
// REUSE THE EXISTING CODE FOR DISPACH SERVICE AND ALL. Do not write custom-logic to send again....

    private final BatchDispatcher batchDispatcher;
    private final ObjectMapper objectMapper;
    private final CampaignService campaignService;
    private final MessageStateService messageStateService;
    private final MessageDispatchRepository messageDispatchRepository;

    @Value("${app.retry.max-attempts}")
    private int maxRetries;

    /**
     * Consumes retry events from the retryable kafla topic. Re-executes the WhatsApp API call directly.
     */
    @KafkaListener(topics = "${app.kafka.topics.campaign-failures-retry}", groupId = "${spring.kafka.consumer.group-id}-retry-worker")
    public void onRetryReceived(@Payload String payload, Acknowledgment acknowledgment) {

        OutboundBatchEvent retryEvent;
        try {
            log.info("Retry worker payload :: {}", payload);
            retryEvent = objectMapper.readValue(payload, OutboundBatchEvent.class);
        } catch (Exception e) {
            log.error("!!! Failed to deserialize retry event. Acknowledging and skipping. Error: {}", e.getMessage());
            e.printStackTrace();
            acknowledgment.acknowledge();
            return;
        }

        try {
            // ── Resolve campaign details ───────────────────────────────────
            Campaign campaign = campaignService.getCampaignDetail(retryEvent.campaignId());
            if (campaign == null) {
                log.error("Campaign detail retrieval failed for campaign [{}] during retry. Routing to DLQ.", retryEvent.campaignId());
                messageStateService.updateBatchFailures(retryEvent, null, null, FailureStatus.CAMPAIGN_DETAIL_RETRIEVAL_FAILED.name(), "Campaign details not found during retry");
                acknowledgment.acknowledge();
                return;
            }

            // ── Check max retries exhausted ────────────────────────────────
            final int maxRetriesBasedOnCampaignBid = maxRetries * campaign.bid();
            List<MessageDispatchDocument> retryExhausted = messageDispatchRepository.findByCampaignIdAndMobileInAndAttemptsGreaterThan(retryEvent.campaignId(), retryEvent.targetPhoneNumbers(), maxRetriesBasedOnCampaignBid);
            if (retryExhausted.size() == retryEvent.targetPhoneNumbers().size()) {
                log.warn("!!! Max retries [{}] exhausted for campaign [{}], batch [{}], targetPhoneNumbers [{}].", maxRetries, retryEvent.campaignId(), retryEvent.batchId());
                messageStateService.updateBatchFailures(retryEvent, null, null, FailureStatus.MAX_RETRIES_EXHAUSTED.name(), "Max retries exhausted (" + maxRetries + ")");
                acknowledgment.acknowledge();

            } else {
                List<MessageDispatchDocument> retryNotExhausted = messageDispatchRepository.findByCampaignIdAndMobileInAndAttemptsLessThanEqual(retryEvent.campaignId(), retryEvent.targetPhoneNumbers(), maxRetries);
                // Extract all the phonennumbrs and call the same dispatch service for sending message (with all the params)
                List<String> targetPhoneNumbers = retryNotExhausted.stream().map(MessageDispatchDocument::getMobile).toList();
                retryEvent = new OutboundBatchEvent(retryEvent.campaignId(), retryEvent.batchId(), targetPhoneNumbers);

                // ── Fan-out HTTP calls via Virtual Threads ──────────────
                batchDispatcher.dispatchRetryBatch(retryEvent, campaign);

                acknowledgment.acknowledge();
            }

        } catch (Exception e) {
            log.error("!!! Failed to process retry event. Acknowledging and skipping. Error: {}", e.getMessage());
            acknowledgment.acknowledge();
        }
    }
}