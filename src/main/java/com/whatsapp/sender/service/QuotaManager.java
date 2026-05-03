package com.whatsapp.sender.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;
import com.whatsapp.sender.dto.Campaign.WabaNumberDetail;
import com.whatsapp.sender.dto.QuotaCheckResult;
import com.whatsapp.sender.dto.QuotaCheckResult.ExhaustionType;
import com.whatsapp.sender.util.Utils;

/**
 * Consolidated Distributed Quota & Circuit Breaker Manager.
 * <p>
 * This is the <strong>single source of truth</strong> for all quota checking,
 * incrementing, and circuit breaker operations. No other service should
 * directly call CircuitBreaker or manipulate Redis quota keys.
 *
 * <h3>Pre-Send Resolution Sequence:</h3>
 * <ol>
 *   <li><strong>Template Check</strong>: Is this template exhausted?
 *       → Rotate to next template.
 *       → If ALL templates exhausted → open per-template circuits with
 *         {@code sendingLimitResetInSeconds}, return exhausted.</li>
 *   <li><strong>Daily Quota Check (80007)</strong>: Is this WABA's daily
 *       quota hit? → Rotate to next WABA.
 *       → If ALL WABAs exhausted → return exhausted.</li>
 *   <li><strong>Burst/MPS Check (130429)</strong>: Is this phone number's
 *       burst circuit open? → Rotate to next phone number.
 *       → If ALL phone numbers hit burst limit → return exhausted.</li>
 * </ol>
 *
 * <h3>Post-Send Increment:</h3>
 * On successful send, {@link #recordSuccessAndCheckLimits} atomically:
 * <ul>
 *   <li>Increments the template quota counter → if at limit, opens template circuit</li>
 *   <li>Increments the WABA quota counter → if at limit, opens daily quota circuit</li>
 *   <li>Increments the campaign success counter</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QuotaManager {

    private static final String TEMPLATE_QUOTA_USED_COUNTER_KEY = "waba:%s:template:%s:quota:used";
    private static final String CAMPAIGN_WABA_SUCCESS_KEY = "campaign:%s:waba:%s:success";

    private final StringRedisTemplate redisTemplate;
    private final CircuitBreaker circuitBreaker;

    /**
     * Resolves the best available combination of template + WABA + phone number.
     * <p>
     * Follows the user-defined sequence:
     * <ol>
     *   <li>Template check → rotate → all exhausted?</li>
     *   <li>Daily quota (80007) check → rotate → all exhausted?</li>
     *   <li>Burst/MPS (130429) check → rotate → all hit?</li>
     * </ol>
     */
    public QuotaCheckResult resolveCombination(Campaign campaign) {
        final String campaignId = String.valueOf(campaign.id());
        final String wabaId = campaign.wabaId();

        // ── Step 1: Resolve Template ───────────────────────────────────────
        TemplateDetail resolvedTemplate = resolveTemplate(wabaId, campaign.templates());
        if (resolvedTemplate == null) {
            return QuotaCheckResult.exhausted(ExhaustionType.TEMPLATE, "All templates exhausted for campaign " + campaignId);
        }

        // ── Step 2: Resolve WABA (Daily Quota — 80007) ─────────────────────
        // check if WaBa account is exhausted along with all its numbers (reset in 24 hrs)
        if (circuitBreaker.isDailyQuotaCircuitOpen(wabaId)) {
            return QuotaCheckResult.exhausted(ExhaustionType.WABA, "All WABA phone numbers exhausted (daily quota) for campaign " + campaignId);
        }

        // ── Step 3: Resolve Phone Number (Burst/MPS — 130429) ──────────────
        WabaNumberDetail resolvedWabaNumber = resolveWabaNumber(wabaId, campaign.wabaNumbers());
        if (resolvedWabaNumber == null) {
            return QuotaCheckResult.exhausted(ExhaustionType.BURST, "All WABA phone numbers exhausted (burst/MPS) for campaign " + campaignId);
        }

        return QuotaCheckResult.allowed(resolvedWabaNumber.wabaPhoneNumberId(), resolvedTemplate.templateId());
    }

    /**
     * Step 1: Resolve an available template.
     * <p>
     * For each template in the campaign:
     * <ol>
     *   <li>Check template circuit (campaignId:templateId) → if open, skip</li>
     *   <li>Check template quota count vs limit → if exhausted, open circuit
     *       with {@code sendingLimitResetInSeconds}, skip</li>
     *   <li>If found available → return it</li>
     * </ol>
     * If ALL templates exhausted → return null.
     */
    private TemplateDetail resolveTemplate(String wabaId, List<TemplateDetail> templates) {
        for (TemplateDetail template : templates) {
            String templateId = template.templateId();

            // 1a. Check template circuit
            if (circuitBreaker.isTemplateCircuitOpen(wabaId, templateId)) {
                log.debug("Template [{}] circuit is OPEN. Rotating...", templateId);
                continue;
            }

            // Template is available
            return template;
        }

        // ALL templates exhausted
        log.warn("ALL templates exhausted for wabaId [{}].", wabaId);
        return null;
    }

    /**
     * Step 3: Resolve an available WABA by burst/MPS limit (130429).
     * <p>
     * Re-iterates through WABAs that pass daily quota AND burst circuit check.
     * The burst circuit is scoped per WaBa Phone Number ID.
     */
    private WabaNumberDetail resolveWabaNumber(String wabaId, List<WabaNumberDetail> wabaNumbers) {
        for (WabaNumberDetail wabaNumber : wabaNumbers) {
            final String wabaPhoneNumberId = wabaNumber.wabaPhoneNumberId();

            // Check burst/MPS circuit (130429) — scoped per Phone Number ID
            if (circuitBreaker.isBurstLimitCircuitOpen(wabaPhoneNumberId)) {
                log.debug("WaBa phone number [{}] burst circuit is OPEN. Rotating...", wabaPhoneNumberId);
                continue;
            }

            // Passes all checks
            return wabaNumber;
        }

        // ALL phone numbers are burst-limited
        log.warn("ALL WaBa phone numbers burst-limited for waba [{}].", wabaId);
        return null;
    }

    /**
     * Records a successful send and proactively checks if quota limits are now hit.
     * <p>
     * This is the ONLY method that should be called on success. It handles:
     * <ol>
     *   <li>Increment template counter → if at limit → open template circuit</li>
     *   <li>Increment campaign success counter</li>
     * </ol>
     */
    public void recordSuccessAndCheckLimits(Campaign campaign, String campaignId, String wabaId, String templateId) {
        // increment success count for campaign + waba
        incrementCounter(String.format(CAMPAIGN_WABA_SUCCESS_KEY, campaignId, wabaId));

        final String quotaUsageRedisKey = String.format(TEMPLATE_QUOTA_USED_COUNTER_KEY, wabaId, templateId);
        // increment template usage counter
        final long newTemplateCount = incrementCounter(quotaUsageRedisKey);

        TemplateDetail templateDetail = Utils.findTemplateDetail(campaign, templateId);
        if (templateDetail != null && newTemplateCount >= templateDetail.quota()) {
            circuitBreaker.openTemplateCircuit(campaignId, templateId, templateDetail.quotaResetInSeconds());
            // Deleting the quota usage information as circuit is open. Once circuit is closed after sometime, quota counter key will be back by increment method
            redisTemplate.delete(quotaUsageRedisKey);
            log.debug("Template [{}] quota REACHED ({}/{}) after increment. Circuit opened for {} seconds.", templateId, newTemplateCount, templateDetail.quota(), templateDetail.quotaResetInSeconds());
        }
    }

    /**
     * Handles a retryable API error by opening the appropriate circuit breaker.
     * <p>
     * This is the ONLY method that should be called when a retryable error occurs.
     * Centralizes all circuit breaker opening logic.
     *
     * @param errorCode         the resolved error code (e.g., "130429", "80007")
     */
    public void handleRetryableError(String errorCode, String wabaId, String wabaPhoneNumberId) {
        if ("130429".equals(errorCode)) {
            circuitBreaker.openBurstLimitCircuit(wabaPhoneNumberId, 60L);
        } else if ("80007".equals(errorCode)) {
            circuitBreaker.openDailyQuotaCircuit(wabaId);
        }
        // 5xx: no persistent circuit breaker (transient, handled by outbox retry delay)
    }

    /**
     * Atomically increments a counter and returns the new value.
     * Sets TTL on first creation to auto-expire at end of day.
     */
    private long incrementCounter(String key) {
        try {
            Long newValue = redisTemplate.opsForValue().increment(key);
            return newValue != null ? newValue : 0L;

        } catch (Exception e) {
            log.error("Failed to increment counter for key : [{}] | Exception : {}", key, e.getMessage());
            return 0L;
        }
    }
}