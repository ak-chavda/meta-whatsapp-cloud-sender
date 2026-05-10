package com.whatsapp.sender.retry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.FailureEvent;
import com.whatsapp.sender.dto.MessageStatusResultEvent;
import com.whatsapp.sender.enums.FailureStatus;
import com.whatsapp.sender.repository.MessageStateRepository;
import com.whatsapp.sender.service.CampaignClient;
import com.whatsapp.sender.service.CampaignService;
import com.whatsapp.sender.service.QuotaManager;
import com.whatsapp.sender.service.WhatsappApiClient;
import com.whatsapp.sender.util.Utils;

/**
 * Retry Worker -- Kafka consumer for the rretryable events kafka topic.
 * Consumes retryable failure events that were pushed by the {@link MetaErrorSchedulers}
 * from the MongoDB outbox after their retry-after time elapsed.
 * <p>
 * Processing flow:
 * <ol>
 *   <li><strong>Deserialize</strong> the {@link FailureEvent} from the retry topic.</li>
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

    private final ObjectMapper objectMapper;
    private final WhatsappApiClient whatsappApiClient;
    private final CampaignService campaignService;
    private final CampaignClient campaignClient;
    private final QuotaManager quotaManager;
    private final MetaErrorOutboxService metaErrorOutboxService;
    private final MessageStateRepository messageStateRepository;
    private final DlqService dlqService;
    private final ExecutorService virtualThreadExecutor;

    @Value("${app.retry.max-attempts}")
    private int maxRetries;

    /**
     * Consumes retry events from the retryable kafla topic. Re-executes the WhatsApp API call directly.
     */
    @KafkaListener(topics = "${app.kafka.topics.campaign-failures-retry}", groupId = "${spring.kafka.consumer.group-id}-retry-worker")
    public void onRetryReceived(@Payload String payload, Acknowledgment acknowledgment) {

        FailureEvent failureEvent;
        try {
            failureEvent = objectMapper.readValue(payload, FailureEvent.class);
        } catch (Exception e) {
            log.error("!!! Failed to deserialize retry event. Acknowledging and skipping. Error: {}", e.getMessage());
            acknowledgment.acknowledge();
            return;
        }

        log.info("Processing retry for campaign [{}], batch [{}], {} targets. Error: {} | Retry: {}/{}",
                failureEvent.campaignId(), failureEvent.batchId(),
                failureEvent.targetPhoneNumbers().size(),
                failureEvent.errorCode(),
                failureEvent.currentRetryCount(), maxRetries);

        try {
            // ── Check max retries exhausted ────────────────────────────────
            if (failureEvent.currentRetryCount() >= maxRetries) {
                log.warn("!!! Max retries ({}) exhausted for campaign [{}], batch [{}]. Routing to DLQ.", maxRetries, failureEvent.campaignId(), failureEvent.batchId());
                saveAsPermanentFailure(failureEvent, FailureStatus.MAX_RETRIES_EXHAUSTED, "Max retries exhausted (" + maxRetries + ")");
                dlqService.routeToDlq(failureEvent, "Max retries exhausted (" + maxRetries + ")");
                acknowledgment.acknowledge();
                return;
            }

            // ── Resolve campaign details ───────────────────────────────────
            Campaign campaign = campaignService.getCampaignDetail(failureEvent.campaignId());
            if (campaign == null) {
                log.error("Campaign detail retrieval failed for campaign [{}] during retry. Routing to DLQ.", failureEvent.campaignId());
                saveAsPermanentFailure(failureEvent, FailureStatus.CAMPAIGN_DETAIL_RETRIEVAL_FAILED, "Campaign details not found during retry");
                dlqService.routeToDlq(failureEvent, "Campaign details not found during retry");
                acknowledgment.acknowledge();
                return;
            }

            // ── Fetch access token ─────────────────────────────────────────
            final String wabaPhoneNumberId = failureEvent.wabaPhoneNumberId();
            final String accessToken = campaignClient.fetchTokenForWhatsappAccount(wabaPhoneNumberId);
            if (accessToken == null || accessToken.isEmpty()) {
                log.error("Access token retrieval failed for WaBa [{}] during retry. Routing to DLQ.", wabaPhoneNumberId);
                saveAsPermanentFailure(failureEvent, FailureStatus.ACCESS_TOKEN_RETRIEVAL_FAILED, "Access token not found for WaBa during retry: " + wabaPhoneNumberId);
                dlqService.routeToDlq(failureEvent, "Access token retrieval failed during retry");
                acknowledgment.acknowledge();
                return;
            }

            // ── Fan-out via Virtual Threads ────────────────────────────────
            // Use the same wabaPhoneNumerId & templateId as it is in failureEvent for retry request 
            final int nextRetryCount = failureEvent.currentRetryCount() + 1;
            List<CompletableFuture<Void>> futures = failureEvent.targetPhoneNumbers().stream()
                    .map(targetPhone -> CompletableFuture.runAsync(() ->
                            retryTarget(failureEvent, campaign, targetPhone, accessToken, nextRetryCount), virtualThreadExecutor))
                    .toList();

            // Barrier: wait for all retries to complete
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            acknowledgment.acknowledge();

            log.info("Retry batch completed for campaign [{}], batch [{}], {} targets.", failureEvent.campaignId(), failureEvent.batchId(), failureEvent.targetPhoneNumbers().size());

        } catch (Exception e) {
            log.error("Unhandled exception during retry for campaign [{}], batch [{}]: {}. NOT acknowledging to kafka.", failureEvent.campaignId(), failureEvent.batchId(), e.getMessage());
        }
    }

    /**
     * Retries a single target phone number. Re-executes the HTTP call and routes the result.
     * <p>
     * All quota/circuit operations delegated to {@link QuotaManager}.
     */
    private void retryTarget(FailureEvent failureEvent, Campaign campaign, String targetPhone, String accessToken, int nextRetryCount) {

        final String wabaId = failureEvent.wabaId();
        final String wabaPhoneNumberId = failureEvent.wabaPhoneNumberId();
        final String templateId = failureEvent.templateId();

        try {
            WhatsappApiClient.SendResult sendResult = whatsappApiClient.sendMessage(wabaPhoneNumberId, templateId, accessToken, targetPhone, campaign);

            if (sendResult.success()) { // ── Success: QuotaManager handles increment + circuit check ──
                quotaManager.recordSuccessAndCheckLimits(campaign, templateId);

                MessageStatusResultEvent successEvent = new MessageStatusResultEvent(
                        failureEvent.batchId(),
                        failureEvent.campaignId(),
                        List.of(targetPhone),
                        true,
                        null, null,
                        sendResult.whatsappMessageId(),
                        nextRetryCount,
                        Instant.now()
                );

                messageStateRepository.saveDispatchResult(successEvent, wabaPhoneNumberId, templateId);
                log.info("Retry succeeded for phone [{}], campaign [{}].", targetPhone, failureEvent.campaignId());

            } else if (Utils.isRetryable(sendResult)) { // ── Retryable failure: QuotaManager handles circuit opening ──
                final String errorCode = Utils.resolveErrorCode(sendResult);
                quotaManager.handleRetryableError(errorCode, wabaId, wabaPhoneNumberId);

                metaErrorOutboxService.queueForRetry(
                        failureEvent.campaignId(),
                        failureEvent.batchId(),
                        wabaId,
                        wabaPhoneNumberId,
                        templateId,
                        List.of(targetPhone),
                        errorCode,
                        sendResult.errorDetail(),
                        nextRetryCount
                );
                log.warn("Retry failed (retryable) for phone [{}]. Error: {}. Re-queued to outbox.", targetPhone, errorCode);

            } else { // ── Non-retryable failure: permanent failure ───────────────
                MessageStatusResultEvent failedEvent = new MessageStatusResultEvent(
                        failureEvent.batchId(),
                        failureEvent.campaignId(),
                        List.of(targetPhone),
                        false,
                        sendResult.errorCode(),
                        sendResult.errorDetail(),
                        null,
                        nextRetryCount,
                        Instant.now()
                );

                messageStateRepository.saveDispatchResult(failedEvent, wabaPhoneNumberId, templateId);
                log.error("Retry failed (non-retryable) for phone [{}]. Error: {}. Saved as permanent failure.", targetPhone, sendResult.errorCode());
            }

        } catch (Exception e) { // Re-queue to outbox as a safety net
            log.error("!!! Exception retrying phone [{}] for campaign [{}]: {}", targetPhone, failureEvent.campaignId(), e.getMessage());
            metaErrorOutboxService.queueForRetry(
                    failureEvent.campaignId(),
                    failureEvent.batchId(),
                    wabaId,
                    wabaPhoneNumberId,
                    templateId,
                    List.of(targetPhone),
                    "CLIENT_ERROR",
                    "Exception during retry: " + e.getMessage(),
                    nextRetryCount
            );
        }
    }

    /**
     * Saves a failure event as a permanent failure to MessageDispatchDocument.
     */
    private void saveAsPermanentFailure(FailureEvent failureEvent, FailureStatus status, String reason) {
        MessageStatusResultEvent terminalFailure = new MessageStatusResultEvent(
                failureEvent.batchId(),
                failureEvent.campaignId(),
                failureEvent.targetPhoneNumbers(),
                false,
                status.name(),
                reason + " | Original: " + failureEvent.errorMessage(),
                null,
                failureEvent.currentRetryCount(),
                Instant.now()
        );

        messageStateRepository.saveDispatchResult(terminalFailure, failureEvent.wabaPhoneNumberId(), failureEvent.templateId());
    }
}