package com.whatsapp.sender.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.OutboundBatchEvent;
import com.whatsapp.sender.dto.QuotaCheckResult;
import com.whatsapp.sender.enums.FailureStatus;
import com.whatsapp.sender.util.Utils;

/**
 * Fans out WhatsApp HTTP calls across Virtual Threads for a consumed batch.
 * <p>
 * For a batch of N target phone numbers, this dispatcher:
 * <ol>
 *   <li>Resolves campaign details from the Redis cache layer (quotas, template IDs).</li>
 *   <li>Delegates quota + circuit breaker checks to {@link QuotaManager} which follows the sequence: Template → Daily Quota (80007) → Burst/MPS (130429).</li>
 *   <li>Fetches the access token from the external service API (never cached).</li>
 *   <li>Submits N {@link CompletableFuture} tasks to the virtual thread executor, each executing a blocking HTTP call to the WhatsApp Cloud API.</li>
 *   <li>Waits for ALL N futures to complete using {@link CompletableFuture#allOf}.</li>
 *   <li>On success: {@link QuotaManager#recordSuccessAndCheckLimits} handles quota increment + circuit breaker opening in one call.</li>
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
    private final MessageStateService messageStateService;
    private final ExecutorService virtualThreadExecutor;

    /**
     * Processes all target phone numbers in a batch concurrently using Virtual
     * Threads.
     */
    public void dispatchBatch(OutboundBatchEvent batchEvent) {

        final Integer campaignId = batchEvent.campaignId();
        final Integer batchId = batchEvent.batchId();
        List<String> targetPhoneNumbers = batchEvent.targetPhoneNumbers();

        log.debug("Dispatching batch [{}] for campaign [{}]. Targets: {}", batchId, campaignId, targetPhoneNumbers.size());

        // ── Validate campaign details ────────────────────
        Campaign campaign = campaignService.getCampaignDetail(campaignId);
        if (campaign == null) {
            log.error("Campaign detail retrieval failed for campaign [{}]. Marking all {} targets as FAILED.", campaignId, targetPhoneNumbers.size());
            messageStateService.insertBatchFailures(batchEvent, null, null, FailureStatus.CAMPAIGN_DETAIL_RETRIEVAL_FAILED.name(), "Campaign details not found");
            return;
        }

        QuotaCheckResult quotaResult = quotaManager.checkDailyQuotaCircuitOpen(campaign.wabaId());
        if (quotaResult != null) {
            log.warn("WABA account daily quota exhausted. Skipping further processing for batch. | campaignId: {}, batchId: {}, exhaustionType: {}, exhaustionReason: {}", campaignId, batchId, quotaResult.exhaustionType(), quotaResult.reason());
            messageStateService.insertBatchFailures(batchEvent, quotaResult.wabaPhoneNumberId(), quotaResult.templateId(), quotaResult.exhaustionType().name(), quotaResult.reason());
            return; // No need to go further as main WABA is exhausted
        }

        // ── Fan-out via Virtual Threads ────────────────────────────
        List<CompletableFuture<Boolean>> futures = targetPhoneNumbers.stream()
                .map(targetPhoneNumber -> CompletableFuture.supplyAsync(
                        () -> processTarget(batchEvent, targetPhoneNumber, campaign), virtualThreadExecutor))
                .toList();

        // Barrier: wait for ALL HTTP calls to complete
        CompletableFuture<Void> allCompleted = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));

        try {
            allCompleted.join();
        } catch (Exception e) {
            log.error("===> Unexpected error waiting for batch [{}] completion: {}", batchId, e.getMessage(), e);
        }

        // Collect results for logging
        List<Boolean> results = futures.stream().map(CompletableFuture::join).toList();
        long successCount = results.stream().filter(Boolean::booleanValue).count();
        long failedCount = results.size() - successCount;

        log.info("===> Batch [{}] completed. Campaign: [{}], Sent: {}, Failed: {}, Total: {}", batchId, campaignId, successCount, failedCount, results.size());
    }

    /**
     * Processes all target phone numbers in a retry-batch concurrently using Virtual
     * Threads.
     */
    public void dispatchRetryBatch(OutboundBatchEvent batchEvent, Campaign campaign) {

        final Integer campaignId = batchEvent.campaignId();
        final Integer batchId = batchEvent.batchId();
        List<String> targetPhoneNumbers = batchEvent.targetPhoneNumbers();

        QuotaCheckResult quotaResult = quotaManager.checkDailyQuotaCircuitOpen(campaign.wabaId());
        if (quotaResult != null) {
            log.warn("WABA account daily quota exhausted. Skipping further processing for retry-batch. | campaignId: {}, batchId: {}, exhaustionType: {}, exhaustionReason: {}", campaignId, batchId, quotaResult.exhaustionType(), quotaResult.reason());
            messageStateService.updateBatchFailures(batchEvent, quotaResult.wabaPhoneNumberId(), quotaResult.templateId(), quotaResult.exhaustionType().name(), quotaResult.reason());
            return; // No need to go further as main WABA is exhausted
        }

        // ── Fan-out via Virtual Threads ────────────────────────────
        List<CompletableFuture<Boolean>> futures = targetPhoneNumbers.stream()
                .map(targetPhoneNumber -> CompletableFuture.supplyAsync(
                        () -> processTarget(batchEvent, targetPhoneNumber, campaign), virtualThreadExecutor))
                .toList();

        // Barrier: wait for ALL HTTP calls to complete
        CompletableFuture<Void> allCompleted = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));

        try {
            allCompleted.join();
        } catch (Exception e) {
            log.error("===> Unexpected error waiting for retry-batch [{}] completion: {}", batchId, e.getMessage(), e);
        }

        // Collect results for logging
        List<Boolean> results = futures.stream().map(CompletableFuture::join).toList();
        long successCount = results.stream().filter(Boolean::booleanValue).count();
        long failedCount = results.size() - successCount;

        log.info("===> Retry-batch [{}] completed. Campaign: [{}], Sent: {}, Failed: {}, Total: {}", batchId, campaignId, successCount, failedCount, results.size());
    }

    /**
     * Processes a single target phone number.
     * <p>
     * Sequence: QuotaManager.resolveCombination() → fetch token → HTTP call → route result.
     * <p>
     * All quota incrementing and circuit breaker operations are delegated to
     * {@link QuotaManager} -- this class does NOT directly call {@link CircuitBreaker}.
     */
    private boolean processTarget(OutboundBatchEvent batchEvent, String targetPhoneNumber, Campaign campaign) {

        QuotaCheckResult quotaResult = quotaManager.resolveCombination(campaign);

        // If NOT allowed
        if (!quotaResult.allowed()) {
            log.warn("Quota exhausted. Type: {}, Reason: {}", quotaResult.exhaustionType(), quotaResult.reason());
            messageStateService.updateFailureForMobile(batchEvent.campaignId(), batchEvent.batchId(), targetPhoneNumber, null, null, quotaResult.exhaustionType().name(), quotaResult.reason());
            return false;
        }

        final String resolvedWabaPhoneNumberId = quotaResult.wabaPhoneNumberId();
        final String resolvedTemplateId = quotaResult.templateId();

        // ── Fetch access token from external service API (never cached) ────
        String accessToken = campaignClient.fetchTokenForWhatsappAccount(resolvedWabaPhoneNumberId);
        if (accessToken == null || accessToken.isEmpty()) {
            log.error("Access token retrieval failed for WaBa [{}]. Saving as permanent failure.", resolvedWabaPhoneNumberId);
            messageStateService.updateFailureForMobile(batchEvent.campaignId(), batchEvent.batchId(), targetPhoneNumber, resolvedWabaPhoneNumberId, resolvedTemplateId, FailureStatus.ACCESS_TOKEN_RETRIEVAL_FAILED.name(), "Access token not found");
            return false;
        }

        // ── Execute HTTP Call ──────────────────────────────────────────────
        WhatsappApiClient.SendResult sendResult = whatsappApiClient.sendMessage(resolvedWabaPhoneNumberId, resolvedTemplateId, accessToken, targetPhoneNumber, campaign);

        // ── Handle Success ─────────────────────────────────────────────────
        if (sendResult.success()) {
            // QuotaManager handles ALL: increment counters + check limits + open circuits
            quotaManager.recordSuccessAndCheckLimits(campaign, resolvedTemplateId);
            messageStateService.updateSuccessForMobile(batchEvent.campaignId(), batchEvent.batchId(), targetPhoneNumber, resolvedWabaPhoneNumberId, resolvedTemplateId, sendResult.whatsappMessageId());
            return true;
        }

        // ── Handle Retryable Errors (130429, 80007, 429, 5xx) → Outbox ────
        if (Utils.isRetryable(sendResult)) {
            String errorCode = Utils.resolveErrorCode(sendResult);

            // QuotaManager handles circuit breaker opening
            quotaManager.handleRetryableError(errorCode, campaign.wabaId(), resolvedWabaPhoneNumberId);
            messageStateService.updateRetryableFailureForMobile(batchEvent.campaignId(), batchEvent.batchId(), targetPhoneNumber, resolvedWabaPhoneNumberId, resolvedTemplateId, errorCode, sendResult.errorDetail());
            return true; 
        }

        // ── Handle Non-Retryable Errors (4xx, etc.) → Permanent Failure ──
        messageStateService.updateFailureForMobile(batchEvent.campaignId(), batchEvent.batchId(), targetPhoneNumber, resolvedWabaPhoneNumberId, resolvedTemplateId, sendResult.errorCode(), sendResult.errorDetail());

        return false;
    }
}