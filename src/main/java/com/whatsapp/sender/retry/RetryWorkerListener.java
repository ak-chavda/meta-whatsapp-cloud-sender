package com.whatsapp.sender.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.FailureEvent;
import com.whatsapp.sender.dto.MessageStatusResultEvent;
import com.whatsapp.sender.repository.MessageStateRepository;
import com.whatsapp.sender.service.CampaignClient;
import com.whatsapp.sender.service.CampaignService;
import com.whatsapp.sender.service.QuotaManager;
import com.whatsapp.sender.service.WhatsappApiClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Retry Worker — Kafka consumer for the {@code whatsapp-failures-retry} topic.
 * <p>
 * Consumes retryable failure events that were pushed by the {@link MetaErrorSchedulers}
 * from the MongoDB outbox after their retry-after time elapsed.
 * <p>
 * Processing flow:
 * <ol>
 *   <li><strong>Deserialize</strong> the {@link FailureEvent} from the retry topic.</li>
 *   <li><strong>Check retry count</strong>: If max retries exhausted → save to
 *       {@code message_dispatch_log} as permanent failure + DLQ.</li>
 *   <li><strong>Fetch access token</strong> from the external Campaign Service.</li>
 *   <li><strong>Re-execute the WhatsApp API call</strong> for each target phone number
 *       using Virtual Threads.</li>
 *   <li><strong>On success</strong>: {@link QuotaManager#recordSuccessAndCheckLimits}
 *       handles quota increment + circuit check. Save to MongoDB.</li>
 *   <li><strong>On retryable failure</strong>: {@link QuotaManager#handleRetryableError}
 *       handles circuit opening. Queue back to outbox.</li>
 *   <li><strong>On non-retryable failure</strong>: Save to {@code MessageDispatchDocument}
 *       as permanent failure + DLQ.</li>
 * </ol>
 * <p>
 * All quota/circuit operations are delegated to {@link QuotaManager}.
 * This class does NOT directly call CircuitBreaker.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryWorkerListener {

    private final ObjectMapper objectMapper;
    private final WhatsappApiClient whatsappApiClient;
    private final CampaignService campaignService;
    private final CampaignClient campaignClient;
    private final QuotaManager quotaManager;
    private final MetaErrorOutboxService metaErrorOutboxService;
    private final MessageStateRepository messageStateRepository;
    private final DlqService dlqService;
    private final ExecutorService virtualThreadExecutor;

    @Value("${app.retry.max-attempts:3}")
    private int maxRetries;

    /**
     * Consumes retry events from the {@code whatsapp-failures-retry} topic.
     * Re-executes the WhatsApp API call directly — does NOT re-publish to
     * the main ingestion topic.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.whatsapp-failures-retry}",
            groupId = "${spring.kafka.consumer.group-id}-retry-worker"
    )
    public void onRetryReceived(@Payload String payload, Acknowledgment acknowledgment) {

        FailureEvent failureEvent;
        try {
            failureEvent = objectMapper.readValue(payload, FailureEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialize retry event. Acknowledging and skipping. Error: {}", e.getMessage());
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
                log.warn("Max retries ({}) exhausted for campaign [{}], batch [{}]. Routing to DLQ.",
                        maxRetries, failureEvent.campaignId(), failureEvent.batchId());
                saveAsPermanentFailure(failureEvent, "Max retries exhausted (" + maxRetries + ")");
                dlqService.routeToDlq(failureEvent, "Max retries exhausted (" + maxRetries + ")");
                acknowledgment.acknowledge();
                return;
            }

            // ── Resolve campaign details ───────────────────────────────────
            Campaign campaign = campaignService.getCampaignDetail(failureEvent.campaignId());
            if (campaign == null) {
                log.error("Campaign detail retrieval failed for campaign [{}] during retry. Routing to DLQ.", failureEvent.campaignId());
                saveAsPermanentFailure(failureEvent, "Campaign details not found during retry");
                dlqService.routeToDlq(failureEvent, "Campaign details not found during retry");
                acknowledgment.acknowledge();
                return;
            }

            // ── Fetch access token ─────────────────────────────────────────
            String wabaPhoneNumberId = failureEvent.wabaPhoneNumberId();
            String accessToken = campaignClient.fetchTokenForWhatsappAccount(wabaPhoneNumberId);
            if (accessToken == null || accessToken.isEmpty()) {
                log.error("Access token retrieval failed for WaBa [{}] during retry. Routing to DLQ.", wabaPhoneNumberId);
                saveAsPermanentFailure(failureEvent, "Access token not found for WaBa during retry: " + wabaPhoneNumberId);
                dlqService.routeToDlq(failureEvent, "Access token retrieval failed during retry");
                acknowledgment.acknowledge();
                return;
            }

            // ── Fan-out via Virtual Threads ────────────────────────────────
            String templateId = failureEvent.templateId();
            String wabaId = failureEvent.wabaId();
            int nextRetryCount = failureEvent.currentRetryCount() + 1;

            List<CompletableFuture<Void>> futures = failureEvent.targetPhoneNumbers().stream()
                    .map(targetPhone -> CompletableFuture.runAsync(() ->
                            retryTarget(failureEvent, campaign, targetPhone, wabaId, wabaPhoneNumberId, templateId, accessToken, nextRetryCount),
                            virtualThreadExecutor))
                    .toList();

            // Barrier: wait for all retries to complete
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            acknowledgment.acknowledge();

            log.info("Retry batch completed for campaign [{}], batch [{}], {} targets.",
                    failureEvent.campaignId(), failureEvent.batchId(),
                    failureEvent.targetPhoneNumbers().size());

        } catch (Exception ex) {
            log.error("Unhandled exception during retry for campaign [{}], batch [{}]: {}. NOT acknowledging.",
                    failureEvent.campaignId(), failureEvent.batchId(), ex.getMessage(), ex);
        }
    }

    /**
     * Retries a single target phone number. Re-executes the HTTP call and routes the result.
     * <p>
     * All quota/circuit operations delegated to {@link QuotaManager}.
     */
    private void retryTarget(
            FailureEvent failureEvent,
            Campaign campaign,
            String targetPhone,
            String wabaId,
            String wabaPhoneNumberId,
            String templateId,
            String accessToken,
            int nextRetryCount
    ) {
        try {
            WhatsappApiClient.SendResult sendResult = whatsappApiClient.sendMessage(
                    wabaPhoneNumberId, templateId, accessToken, targetPhone, campaign);

            if (sendResult.success()) {
                // ── Success: QuotaManager handles increment + circuit check ──
                quotaManager.recordSuccessAndCheckLimits(campaign, failureEvent.campaignId(), wabaPhoneNumberId, wabaId, templateId);

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
                log.debug("Retry succeeded for phone [{}], campaign [{}].", targetPhone, failureEvent.campaignId());

            } else if (isRetryable(sendResult)) {
                // ── Retryable failure: QuotaManager handles circuit opening ──
                String errorCode = resolveErrorCode(sendResult);
                quotaManager.handleRetryableError(errorCode, wabaId, wabaPhoneNumberId, sendResult.retryAfterSeconds());

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

                log.debug("Retry failed (retryable) for phone [{}]. Error: {}. Re-queued to outbox.", targetPhone, errorCode);

            } else {
                // ── Non-retryable failure: permanent failure ───────────────
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
                log.warn("Retry failed (non-retryable) for phone [{}]. Error: {}. Saved as permanent failure.", targetPhone, sendResult.errorCode());
            }

        } catch (Exception ex) {
            log.error("Exception retrying phone [{}] for campaign [{}]: {}",
                    targetPhone, failureEvent.campaignId(), ex.getMessage(), ex);
            // Re-queue to outbox as a safety net
            metaErrorOutboxService.queueForRetry(
                    failureEvent.campaignId(),
                    failureEvent.batchId(),
                    wabaId,
                    wabaPhoneNumberId,
                    templateId,
                    List.of(targetPhone),
                    "CLIENT_ERROR",
                    "Exception during retry: " + ex.getMessage(),
                    nextRetryCount
            );
        }
    }

    /**
     * Determines if a send result is retryable.
     */
    private boolean isRetryable(WhatsappApiClient.SendResult sendResult) {
        if (sendResult.httpStatusCode() == 429 || sendResult.httpStatusCode() >= 500) {
            return true;
        }
        String errorCode = sendResult.errorCode();
        if (errorCode != null) {
            return errorCode.equals("META_130429") || errorCode.equals("META_80007");
        }
        return false;
    }

    /**
     * Resolves the error code for outbox storage (strips META_ prefix if present).
     */
    private String resolveErrorCode(WhatsappApiClient.SendResult sendResult) {
        String errorCode = sendResult.errorCode();
        if (errorCode != null && errorCode.startsWith("META_")) {
            return errorCode.substring(5);
        }
        return errorCode;
    }

    /**
     * Saves a failure event as a permanent failure to MessageDispatchDocument.
     */
    private void saveAsPermanentFailure(FailureEvent failureEvent, String reason) {
        MessageStatusResultEvent terminalFailure = new MessageStatusResultEvent(
                failureEvent.batchId(),
                failureEvent.campaignId(),
                failureEvent.targetPhoneNumbers(),
                false,
                failureEvent.errorCode(),
                reason + " | Original: " + failureEvent.errorMessage(),
                null,
                failureEvent.currentRetryCount(),
                Instant.now()
        );

        messageStateRepository.saveDispatchResult(terminalFailure, failureEvent.wabaPhoneNumberId(), failureEvent.templateId());
    }
}