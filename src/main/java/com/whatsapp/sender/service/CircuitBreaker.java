package com.whatsapp.sender.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Redis-backed Soft Circuit Breaker.
 * Implements a "Drop & Route" strategy with two scopes:
 * <ul>
 *   <li><strong>Per Phone Number ID (130429 — Burst/MPS Limit):</strong>
 *       When a 130429 is encountered, the circuit opens for that specific
 *       phone number ID. Other phone numbers under the same WABA remain unaffected.</li>
 *   <li><strong>Per WABA ID (80007 — Daily Quota Limit):</strong>
 *       When an 80007 is encountered for ANY phone number under a WABA,
 *       the circuit opens for the entire WABA. All phone numbers under
 *       that WABA share the same daily quota pool.</li>
 *   <li><strong>Per Campaign (Template/WaBa Exhaustion):</strong>
 *       Campaign-level circuit when all templates or all WABAs are exhausted.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CircuitBreaker {

    /** Per Phone Number ID — burst/MPS limit (130429). */
    private static final String WABA_RATE_LIMIT_PREFIX = "circuit:waba:rate-limit:";

    /** Per WABA ID — daily quota limit (80007). All phone numbers share this pool. */
    private static final String WABA_DAILY_QUOTA_PREFIX = "circuit:waba:daily-quota:";

    /** Per Campaign — any template exhausted. */
    private static final String CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX = "circuit:campaign:template:exhausted:";

    private final StringRedisTemplate redisTemplate;

    public boolean isTemplateCircuitOpen(String campaignId, String templateId) {
        return isCircuitOpen(CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX, campaignId + ":" + templateId);
    }

    public void openTemplateCircuit(String campaignId, String templateId, long retryAfterSeconds) {
        openCircuit(CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX, campaignId + ":" + templateId, retryAfterSeconds);
    }

    /**
     * Checks if the daily quota circuit is open for a WABA.
     */
    public boolean isDailyQuotaCircuitOpen(String wabaId) {
        return isCircuitOpen(WABA_DAILY_QUOTA_PREFIX, wabaId);
    }

    /**
     * Opens the WABA daily quota circuit (80007 scope).
     * When any phone number under a WABA hits error code 80007,
     * this blocks ALL phone numbers under that WABA for 24 hours.
     */
    public void openDailyQuotaCircuit(String wabaId) {
        openCircuit(WABA_DAILY_QUOTA_PREFIX, wabaId, 86400L); // 24 hours
    }

    /**
     * Checks if the burst/MPS circuit (130429 scope) for a specific waba-phone-number-id.
     */
    public boolean isBurstLimitCircuitOpen(String wabaPhoneNumberId) {
        return isCircuitOpen(WABA_RATE_LIMIT_PREFIX, wabaPhoneNumberId);
    }

    /**
     * Opens the burst/MPS circuit (130429 scope) for a specific waba-phone-number-id.
     */
    public void openBurstLimitCircuit(String wabaPhoneNumberId, long retryAfterSeconds) {
        openCircuit(WABA_RATE_LIMIT_PREFIX, wabaPhoneNumberId, retryAfterSeconds);
    }

    /**
     * Checks if the circuit is open (rate limited) for the given key.
     * <p>
     * Uses {@code hasKey()} to check key existence. When the TTL expires,
     * the key is auto-deleted by Redis, which effectively closes the circuit.
     */
    private boolean isCircuitOpen(String prefix, String id) {
        final String key = prefix + id;
        try {
            return Boolean.TRUE.equals(redisTemplate.hasKey(key));
        } catch (Exception e) {
            log.error("Error checking circuit breaker for key : [{}]. Defaulting to CLOSED. | Exception occurred: {}", key, e.getMessage());
            return false;
        }
    }

    /**
     * Opens the circuit for a specific key for a given duration.
     */
    private void openCircuit(String prefix, String id, long keepCircuitOpenForSeconds) {
        final String key = prefix + id;
        try {
            redisTemplate.opsForValue().set(key, "OPEN", Duration.ofSeconds(keepCircuitOpenForSeconds));
            log.warn("Circuit OPENED for key : [{}]. Paused for {} seconds.", key, keepCircuitOpenForSeconds);
        } catch (Exception ex) {
            log.error("Failed to open circuit breaker for key : [{}]. | Exception occurred: {}", key, ex.getMessage());
        }
    }
}