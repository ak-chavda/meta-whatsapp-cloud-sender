package com.whatsapp.sender.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;
import com.whatsapp.sender.dto.Campaign.WhatsAppBusinessAccountDetail;
import com.whatsapp.sender.dto.QuotaCheckResult;

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

    private static final String WABA_QUOTA_KEY = "quota:waba:%s:daily:%s";
    private static final String TEMPLATE_QUOTA_KEY = "quota:template:%s:daily:%s";
    private static final String SUCCESS_KEY = "campaign:%d:waba:%s:success";
    private static final long DAY_TTL_SECONDS = 86400L;

    private final StringRedisTemplate redisTemplate;
    private final CircuitBreaker circuitBreaker;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  PRE-SEND: Resolve available template + WABA + phone number combination
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        final String campaignIdStr = String.valueOf(campaign.id());

        // ── Step 1: Resolve Template ───────────────────────────────────────
        TemplateDetail resolvedTemplate = resolveTemplate(campaign, campaignIdStr, today);

        if (resolvedTemplate == null) {
            return QuotaCheckResult.exhaustedTemplate("All templates exhausted for campaign " + campaignIdStr);
        }

        // ── Step 2: Resolve WABA (Daily Quota — 80007) ─────────────────────
        WhatsAppBusinessAccountDetail resolvedWaba = resolveWabaByDailyQuota(campaign, today);

        if (resolvedWaba == null) {
            return QuotaCheckResult.exhaustedWaba("All WABAs exhausted (daily quota) for campaign " + campaignIdStr);
        }

        // ── Step 3: Resolve Phone Number (Burst/MPS — 130429) ──────────────
        // The resolved WABA from step 2 already passed the daily quota check.
        // Now verify it isn't burst-limited.
        WhatsAppBusinessAccountDetail resolvedWabaWithBurst = resolveWabaByBurstLimit(campaign, today);

        if (resolvedWabaWithBurst == null) {
            return QuotaCheckResult.exhaustedBurst("All WaBa phone numbers burst-limited (130429) for campaign " + campaignIdStr);
        }

        return QuotaCheckResult.allowed(
                resolvedWabaWithBurst.waBaId(),
                resolvedWabaWithBurst.waBaPhoneNumberId(),
                resolvedTemplate.templateId()
        );
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
    private TemplateDetail resolveTemplate(Campaign campaign, String campaignIdStr, String today) {
        List<TemplateDetail> templates = campaign.templateDetails();

        for (TemplateDetail template : templates) {
            String templateId = template.templateId();

            // 1a. Check template circuit
            if (circuitBreaker.isTemplateCircuitOpen(campaignIdStr, templateId)) {
                log.debug("Template [{}] circuit is OPEN for campaign [{}]. Rotating...", templateId, campaignIdStr);
                continue;
            }

            // 1b. Check template quota
            long templateLimit = template.dailySendingLimit();
            if (templateLimit > 0) {
                long currentCount = getCounter(TEMPLATE_QUOTA_KEY, templateId, today);
                if (currentCount >= templateLimit) {
                    // Proactively open the circuit so future checks skip this template
                    long resetSeconds = template.sendingLimitResetInSeconds() > 0
                            ? template.sendingLimitResetInSeconds()
                            : getSecondsUntilEndOfDay();
                    circuitBreaker.openTemplateCircuit(campaignIdStr, templateId, resetSeconds);
                    log.info("Template [{}] quota exhausted ({}/{}). Circuit opened for {} seconds.",
                            templateId, currentCount, templateLimit, resetSeconds);
                    continue;
                }
            }

            // Template is available
            return template;
        }

        // ALL templates exhausted
        log.warn("ALL templates exhausted for campaign [{}].", campaignIdStr);
        return null;
    }

    /**
     * Step 2: Resolve an available WABA by daily quota (80007).
     * <p>
     * For each WABA in the campaign:
     * <ol>
     *   <li>Check daily quota circuit (wabaId) → if open, skip</li>
     *   <li>Check WABA quota count vs limit → if exhausted, skip</li>
     *   <li>If found available → return it</li>
     * </ol>
     * If ALL WABAs exhausted → return null.
     */
    private WhatsAppBusinessAccountDetail resolveWabaByDailyQuota(Campaign campaign, String today) {
        List<WhatsAppBusinessAccountDetail> wabas = campaign.whatsappBusinessAccountDetails();

        for (WhatsAppBusinessAccountDetail waba : wabas) {
            String wabaId = waba.waBaId();
            String wabaPhoneNumberId = waba.waBaPhoneNumberId();

            // 2a. Check daily quota circuit (80007) — scoped per WABA ID
            if (wabaId != null && circuitBreaker.isDailyQuotaCircuitOpen(wabaId)) {
                log.debug("WABA [{}] daily quota circuit is OPEN. Rotating...", wabaId);
                continue;
            }

            // 2b. Check daily quota count vs limit
            long wabaLimit = waba.dailySendingLimit();
            if (wabaLimit > 0) {
                long currentCount = getCounter(WABA_QUOTA_KEY, wabaPhoneNumberId, today);
                if (currentCount >= wabaLimit) {
                    log.debug("WABA phone [{}] daily quota exhausted ({}/{}). Rotating...",
                            wabaPhoneNumberId, currentCount, wabaLimit);
                    continue;
                }
            } else {
                // Skip misconfigured WABA (no limit defined)
                continue;
            }

            // WABA is available (daily quota OK)
            return waba;
        }

        // ALL WABAs exhausted by daily quota
        log.warn("ALL WABAs daily quota exhausted for campaign [{}].", campaign.id());
        return null;
    }

    /**
     * Step 3: Resolve an available WABA by burst/MPS limit (130429).
     * <p>
     * Re-iterates through WABAs that pass daily quota AND burst circuit check.
     * The burst circuit is scoped per WaBa Phone Number ID.
     */
    private WhatsAppBusinessAccountDetail resolveWabaByBurstLimit(Campaign campaign, String today) {
        List<WhatsAppBusinessAccountDetail> wabas = campaign.whatsappBusinessAccountDetails();

        for (WhatsAppBusinessAccountDetail waba : wabas) {
            String wabaId = waba.waBaId();
            String wabaPhoneNumberId = waba.waBaPhoneNumberId();

            // Re-check daily quota first (same as step 2 — ensures consistency)
            if (wabaId != null && circuitBreaker.isDailyQuotaCircuitOpen(wabaId)) {
                continue;
            }

            long wabaLimit = waba.dailySendingLimit();
            if (wabaLimit > 0) {
                long currentCount = getCounter(WABA_QUOTA_KEY, wabaPhoneNumberId, today);
                if (currentCount >= wabaLimit) {
                    continue;
                }
            } else {
                continue;
            }

            // 3a. Check burst/MPS circuit (130429) — scoped per Phone Number ID
            if (circuitBreaker.isBurstLimitCircuitOpen(wabaPhoneNumberId)) {
                log.debug("WaBa phone [{}] burst circuit is OPEN. Rotating...", wabaPhoneNumberId);
                continue;
            }

            // Passes all checks
            return waba;
        }

        // ALL phone numbers are burst-limited
        log.warn("ALL WaBa phone numbers burst-limited for campaign [{}].", campaign.id());
        return null;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  POST-SEND: Record success + proactively check limits → open circuits
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Records a successful send and proactively checks if quota limits are now hit.
     * <p>
     * This is the ONLY method that should be called on success. It handles:
     * <ol>
     *   <li>Increment template counter → if at limit → open template circuit</li>
     *   <li>Increment WABA counter → if at limit → open daily quota circuit</li>
     *   <li>Increment campaign success counter</li>
     * </ol>
     *
     * @param campaign         the full campaign object (for quota limits & reset config)
     * @param campaignId       campaign identifier
     * @param wabaPhoneNumberId the WaBa phone number used
     * @param wabaId           the WABA account ID (for 80007 circuit scoping)
     * @param templateId       the template used
     */
    public void recordSuccessAndCheckLimits(
            Campaign campaign,
            Integer campaignId,
            String wabaPhoneNumberId,
            String wabaId,
            String templateId
    ) {
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        final String campaignIdStr = String.valueOf(campaignId);

        // ── Increment Template Counter & Check Limit ───────────────────────
        long newTemplateCount = incrementCounter(TEMPLATE_QUOTA_KEY, templateId, today);
        TemplateDetail templateDetail = findTemplateDetail(campaign, templateId);
        if (templateDetail != null && templateDetail.dailySendingLimit() > 0) {
            if (newTemplateCount >= templateDetail.dailySendingLimit()) {
                long resetSeconds = templateDetail.sendingLimitResetInSeconds() > 0
                        ? templateDetail.sendingLimitResetInSeconds()
                        : getSecondsUntilEndOfDay();
                circuitBreaker.openTemplateCircuit(campaignIdStr, templateId, resetSeconds);
                log.info("Template [{}] quota REACHED ({}/{}) after increment. Circuit opened for {} seconds.",
                        templateId, newTemplateCount, templateDetail.dailySendingLimit(), resetSeconds);
            }
        }

        // ── Increment WABA Counter & Check Limit ───────────────────────────
        long newWabaCount = incrementCounter(WABA_QUOTA_KEY, wabaPhoneNumberId, today);
        WhatsAppBusinessAccountDetail wabaDetail = findWabaDetail(campaign, wabaPhoneNumberId);
        if (wabaDetail != null && wabaDetail.dailySendingLimit() > 0) {
            if (newWabaCount >= wabaDetail.dailySendingLimit()) {
                // Don't open daily quota circuit here — that's only for API-returned 80007.
                // Just log so the next resolveCombination() call will see the counter >= limit.
                log.info("WABA phone [{}] daily quota REACHED ({}/{}) after increment.",
                        wabaPhoneNumberId, newWabaCount, wabaDetail.dailySendingLimit());
            }
        }

        // ── Increment Campaign Success Counter ─────────────────────────────
        incrementSuccessCounter(campaignId, wabaPhoneNumberId);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  POST-FAILURE: Handle retryable API errors → open appropriate circuits
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Handles a retryable API error by opening the appropriate circuit breaker.
     * <p>
     * This is the ONLY method that should be called when a retryable error occurs.
     * Centralizes all circuit breaker opening logic.
     *
     * @param errorCode         the resolved error code (e.g., "130429", "80007")
     * @param wabaId            the WABA account ID
     * @param wabaPhoneNumberId the phone number ID
     * @param retryAfterSeconds the Retry-After value from the API (for burst limit)
     */
    public void handleRetryableError(String errorCode, String wabaId, String wabaPhoneNumberId, Long retryAfterSeconds) {
        if ("130429".equals(errorCode)) {
            long retryAfter = (retryAfterSeconds != null && retryAfterSeconds > 0) ? retryAfterSeconds : 60L;
            circuitBreaker.openBurstLimitCircuit(wabaPhoneNumberId, retryAfter);
        } else if ("80007".equals(errorCode)) {
            circuitBreaker.openDailyQuotaCircuit(wabaId);
        }
        // 5xx: no persistent circuit breaker (transient, handled by outbox retry delay)
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  INTERNAL: Redis counter operations
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Gets the current counter value for a given key.
     */
    private long getCounter(String keyPattern, String id, String today) {
        String key = String.format(keyPattern, id, today);
        try {
            String countStr = redisTemplate.opsForValue().get(key);
            return countStr != null ? Long.parseLong(countStr) : 0L;
        } catch (Exception e) {
            log.warn("Failed to get counter [{}]: {}", key, e.getMessage());
            return 0L;
        }
    }

    /**
     * Atomically increments a counter and returns the new value.
     * Sets TTL on first creation to auto-expire at end of day.
     */
    private long incrementCounter(String keyPattern, String id, String today) {
        String key = String.format(keyPattern, id, today);
        try {
            Long newValue = redisTemplate.opsForValue().increment(key);
            // Set TTL only on first increment (when value becomes 1)
            if (newValue != null && newValue == 1) {
                redisTemplate.expire(key, Duration.ofSeconds(DAY_TTL_SECONDS));
            }
            return newValue != null ? newValue : 0L;
        } catch (Exception e) {
            log.warn("Failed to increment counter [{}]: {}", key, e.getMessage());
            return 0L;
        }
    }

    /**
     * Decrements a counter (used when a send fails after quota was already incremented).
     */
    public void decrementQuota(String wabaPhoneNumberId, String templateId) {
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        try {
            redisTemplate.opsForValue().decrement(String.format(WABA_QUOTA_KEY, wabaPhoneNumberId, today));
            redisTemplate.opsForValue().decrement(String.format(TEMPLATE_QUOTA_KEY, templateId, today));
        } catch (Exception e) {
            log.warn("Failed to decrement quota for waba [{}] template [{}]: {}", wabaPhoneNumberId, templateId, e.getMessage());
        }
    }

    /**
     * Increments the campaign success counter (observability-only, non-critical).
     */
    private void incrementSuccessCounter(Integer campaignId, String wabaPhoneNumberId) {
        final String key = String.format(SUCCESS_KEY, campaignId, wabaPhoneNumberId);
        try {
            redisTemplate.opsForValue().increment(key);
        } catch (Exception e) {
            log.warn("Failed to increment success counter for campaign [{}] waba [{}]: {}", campaignId, wabaPhoneNumberId, e.getMessage());
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  INTERNAL: Helpers
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private long getSecondsUntilEndOfDay() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        LocalDateTime endOfDay = now.toLocalDate().atTime(LocalTime.MAX);
        return java.time.Duration.between(now, endOfDay).getSeconds();
    }

    private TemplateDetail findTemplateDetail(Campaign campaign, String templateId) {
        if (campaign.templateDetails() == null) return null;
        return campaign.templateDetails().stream()
                .filter(t -> templateId.equals(t.templateId()))
                .findFirst()
                .orElse(null);
    }

    private WhatsAppBusinessAccountDetail findWabaDetail(Campaign campaign, String wabaPhoneNumberId) {
        if (campaign.whatsappBusinessAccountDetails() == null) return null;
        return campaign.whatsappBusinessAccountDetails().stream()
                .filter(w -> wabaPhoneNumberId.equals(w.waBaPhoneNumberId()))
                .findFirst()
                .orElse(null);
    }
}