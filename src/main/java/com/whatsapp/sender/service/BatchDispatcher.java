package com.whatsapp.sender.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.FailureEvent;
import com.whatsapp.sender.dto.MessageStatusResultEvent;
import com.whatsapp.sender.dto.OutboundBatchEvent;
import com.whatsapp.sender.dto.QuotaCheckResult;
import com.whatsapp.sender.repository.MessageStateRepository;
import com.whatsapp.sender.retry.DlqService;
import com.whatsapp.sender.retry.MetaErrorOutboxService;

/**
 * Fans out WhatsApp HTTP calls across Virtual Threads for a consumed batch.
 * <p>
 * For a batch of N target phone numbers, this dispatcher:
 * <ol>
 *   <li>Resolves campaign details from the Redis cache layer (quotas, template IDs).</li>
 *   <li>Delegates quota + circuit breaker checks to {@link QuotaManager} which
 *       follows the sequence: Template → Daily Quota (80007) → Burst/MPS (130429).</li>
 *   <li>Fetches the access token from the external service API (never cached).</li>
 *   <li>Submits N {@link CompletableFuture} tasks to the virtual thread executor,
 *       each executing a blocking HTTP call to the WhatsApp Cloud API.</li>
 *   <li>Waits for ALL N futures to complete using {@link CompletableFuture#allOf}.</li>
 *   <li>On success: {@link QuotaManager#recordSuccessAndCheckLimits} handles
 *       quota increment + circuit breaker opening in one call.</li>
 *   <li>On retryable failure: queues to {@code MetaErrorOutboxDocument}.</li>
 *   <li>On non-retryable failure: persists to {@code MessageDispatchDocument} + DLQ.</li>
 * </ol>
 * <p>
 * No failure events are published directly to Kafka. The MetaErrorSchedulers
 * handle pushing ripe outbox documents to the retry topic.
 * MongoDB is the sole audit log for all dispatch results.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchDispatcher {

    private final WhatsappApiClient whatsappApiClient;
    private final CampaignService campaignService;
    private final CampaignClient campaignClient;
    private final QuotaManager quotaManager;
    private final MessageStateRepository messageStateRepository;
    private final MetaErrorOutboxService metaErrorOutboxService;
    private final DlqService dlqService;
    private final ExecutorService virtualThreadExecutor;

    /**
     * Processes all target phone numbers in a batch concurrently using Virtual Threads.
     */
    public void dispatchBatch(OutboundBatchEvent batchEvent) {
        Integer campaignId = batchEvent.campaignId();
        Integer batchId = batchEvent.batchId();
        List<String> targetPhoneNumbers = batchEvent.targetPhoneNumbers();

        log.info("Dispatching batch [{}] for campaign [{}]. Targets: {}", batchId, campaignId, targetPhoneNumbers.size());

        // ── Step 1: Validate campaign details ────────────────────
        Campaign campaign = campaignService.getCampaignDetail(campaignId);
        if (campaign == null) {
            log.error("Campaign detail retrieval failed for campaign [{}]. Marking all {} targets as FAILED.", campaignId, targetPhoneNumbers.size());
            handleBatchLevelFailure(batchEvent, "CAMPAIGN_DETAIL_RETRIEVAL_FAILED", "Campaign details not found");
            return;
        }
        
        if (campaign.templates() == null || campaign.templates().isEmpty()) {
            log.error("No template configured for campaign [{}]. Marking all targets as FAILED.", campaignId);
            handleBatchLevelFailure(batchEvent, "NO_TEMPLATE_CONFIGURED", "No template found in campaign configuration");
            return;
        }

        if (campaign.wabaNumbers() == null || campaign.wabaNumbers().isEmpty()) {
            log.error("No WaBa phone number configured for campaign [{}]. Marking all targets as FAILED.", campaignId);
            handleBatchLevelFailure(batchEvent, "NO_WABA_CONFIGURED", "No WaBa phone number found in campaign configuration");
            return;
        }

        // ── Step 2: Fan-out via Virtual Threads ────────────────────────────
        List<CompletableFuture<MessageStatusResultEvent>> futures = targetPhoneNumbers.stream()
                .map(targetPhoneNumber -> CompletableFuture.supplyAsync(() -> processTarget(batchEvent, targetPhoneNumber, campaign), virtualThreadExecutor))
                .toList();

        // Barrier: wait for ALL HTTP calls to complete
        CompletableFuture<Void> allCompleted = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));

        try {
            allCompleted.join();
        } catch (Exception ex) {
            log.error("Unexpected error waiting for batch [{}] completion: {}", batchId, ex.getMessage(), ex);
        }

        // Collect results for logging
        List<MessageStatusResultEvent> results = futures.stream().map(CompletableFuture::join).toList();
        long successCount = results.stream().filter(MessageStatusResultEvent::isSendSuccessful).count();
        long failedCount = results.size() - successCount;

        log.info("Batch [{}] completed. Campaign: [{}], Sent: {}, Failed: {}, Total: {}", batchId, campaignId, successCount, failedCount, results.size());
    }

    /**
     * Processes a single target phone number.
     * <p>
     * Sequence: QuotaManager.resolveCombination() → fetch token → HTTP call → route result.
     * <p>
     * All quota incrementing and circuit breaker operations are delegated to
     * {@link QuotaManager} — this class does NOT directly call {@link CircuitBreaker}.
     */
    private MessageStatusResultEvent processTarget(OutboundBatchEvent batchEvent, String targetPhoneNumber, Campaign campaign) {

        // ── Quota + Circuit Breaker Check (Template → 80007 → 130429) ──────
        QuotaCheckResult quotaResult = quotaManager.resolveCombination(campaign);

        if (!quotaResult.allowed()) {
            log.warn("Quota exhausted. Type: {}, Reason: {}", quotaResult.exhaustionType(), quotaResult.reason());
            MessageStatusResultEvent failedEvent = createStatusEvent(batchEvent, List.of(targetPhoneNumber), false, "QUOTA_EXHAUSTED", quotaResult.reason(), null, 0);
            messageStateRepository.saveDispatchResult(failedEvent, null, null);
            return failedEvent;
        }

        final String resolvedWabaPhoneNumberId = quotaResult.wabaPhoneNumberId();
        final String resolvedTemplateId = quotaResult.templateId();

        // ── Fetch access token from external service API (never cached) ────
        String accessToken = campaignClient.fetchTokenForWhatsappAccount(resolvedWabaPhoneNumberId);
        if (accessToken == null || accessToken.isEmpty()) {
            log.error("Access token retrieval failed for WaBa [{}]. Saving as permanent failure.", resolvedWabaPhoneNumberId);
            MessageStatusResultEvent failedEvent = createStatusEvent(batchEvent, List.of(targetPhoneNumber), false, "TOKEN_RETRIEVAL_FAILED", "Access token not found for WaBa: " + resolvedWabaPhoneNumberId, null, 0);
            messageStateRepository.saveDispatchResult(failedEvent, resolvedWabaPhoneNumberId, resolvedTemplateId);
            return failedEvent;
        }

        // ── Execute HTTP Call ──────────────────────────────────────────────
        WhatsappApiClient.SendResult sendResult = whatsappApiClient.sendMessage(resolvedWabaPhoneNumberId, resolvedTemplateId, accessToken, targetPhoneNumber, campaign);

        // ── Handle Success ─────────────────────────────────────────────────
        if (sendResult.success()) {
            // QuotaManager handles ALL: increment counters + check limits + open circuits
            quotaManager.recordSuccessAndCheckLimits(campaign, batchEvent.campaignId().toString(), campaign.wabaId(), resolvedTemplateId);
            
            MessageStatusResultEvent successEvent = createStatusEvent(batchEvent, List.of(targetPhoneNumber), true, null, null, sendResult.whatsappMessageId(), 0);
            messageStateRepository.saveDispatchResult(successEvent, resolvedWabaPhoneNumberId, resolvedTemplateId);
            return successEvent;
        }

        // ── Handle Retryable Errors (130429, 80007, 429, 5xx) → Outbox ────
        if (isRetryable(sendResult)) {
            String errorCode = resolveErrorCode(sendResult);

            // QuotaManager handles circuit breaker opening
            quotaManager.handleRetryableError(errorCode, campaign.wabaId(), resolvedWabaPhoneNumberId);

            // Queue to MetaErrorOutbox for scheduled retry
            metaErrorOutboxService.queueForRetry(
                    batchEvent.campaignId(),
                    batchEvent.batchId(),
                    campaign.wabaId(),
                    resolvedWabaPhoneNumberId,
                    resolvedTemplateId,
                    List.of(targetPhoneNumber),
                    errorCode,
                    sendResult.errorDetail(),
                    0 // first attempt
            );

            return createStatusEvent(batchEvent, List.of(targetPhoneNumber), false, errorCode, sendResult.errorDetail(), null, 0);
        }

        // ── Handle Non-Retryable Errors (4xx, etc.) → Permanent Failure + DLQ ──
        MessageStatusResultEvent failedEvent = createStatusEvent(batchEvent, List.of(targetPhoneNumber), false, sendResult.errorCode(), sendResult.errorDetail(), null, 0);
        messageStateRepository.saveDispatchResult(failedEvent, resolvedWabaPhoneNumberId, resolvedTemplateId);

        // Route to DLQ for non-retryable errors
        FailureEvent dlqEvent = new FailureEvent(
                batchEvent.campaignId(),
                batchEvent.batchId(),
                campaign.wabaId(),
                resolvedWabaPhoneNumberId,
                resolvedTemplateId,
                List.of(targetPhoneNumber),
                sendResult.errorCode(),
                sendResult.errorDetail(),
                0,
                Instant.now()
        );
        dlqService.routeToDlq(dlqEvent, "Non-retryable error: " + sendResult.errorCode());

        return failedEvent;
    }

    /**
     * Determines if a send result is retryable.
     * Retryable: 429, 5xx, META_130429 (burst/MPS), META_80007 (daily quota).
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
     * Resolves the error code for outbox storage.
     * Strips "META_" prefix for Meta API errors, keeps "HTTP_xxx" for HTTP errors.
     */
    private String resolveErrorCode(WhatsappApiClient.SendResult sendResult) {
        String errorCode = sendResult.errorCode();
        if (errorCode != null && errorCode.startsWith("META_")) {
            return errorCode.substring(5); // "META_130429" → "130429"
        }
        return errorCode; // "HTTP_500" stays as-is
    }

    /**
     * Handles batch-level failures (e.g., campaign detail retrieval failure).
     */
    private void handleBatchLevelFailure(OutboundBatchEvent batch, String errorCode, String errorMessage) {
        MessageStatusResultEvent failedEvent = createStatusEvent(batch, batch.targetPhoneNumbers(), false, errorCode, errorMessage, null, 0);
        messageStateRepository.saveDispatchResult(failedEvent, null, null);
    }

    /**
     * Creates a per-target status result event (internal tracking only, not published to Kafka).
     */
    private MessageStatusResultEvent createStatusEvent(OutboundBatchEvent batch, List<String> targetPhoneNumbers, boolean isSendSuccessful, String errorCode, String errorMessage, String whatsappMessageId, int retryCount) {
        return new MessageStatusResultEvent(
                batch.batchId(),
                batch.campaignId(),
                targetPhoneNumbers,
                isSendSuccessful,
                errorCode,
                errorMessage,
                whatsappMessageId,
                retryCount,
                Instant.now()
        );
    }
}