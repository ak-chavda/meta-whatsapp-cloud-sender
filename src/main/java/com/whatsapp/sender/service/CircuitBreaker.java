package com.whatsapp.sender.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CircuitBreaker {

    /** Per Phone Number ID — burst/MPS limit (130429). */
    private static final String WABA_PHONE_NUMBER_RATE_LIMIT_PREFIX = "circuit:waba:rate-limit:";

    /**
     * Per WABA ID — daily quota limit (80007). All phone numbers share this pool.
     */
    private static final String WABA_DAILY_QUOTA_EXHAUSTED_PREFIX = "circuit:waba:daily-quota:";

    /** Per Campaign — any template exhausted. */
    private static final String CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX = "circuit:waba:%s:template:%s";

    private final StringRedisTemplate redisTemplate;

    public boolean isTemplateCircuitOpen(String wabaId, String templateId) {
        return isCircuitOpen(String.format(CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX, wabaId, templateId));
    }

    public void openTemplateCircuit(String wabaId, String templateId, long retryAfterSeconds) {
        openCircuit(String.format(CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX, wabaId, templateId), retryAfterSeconds);
    }

    /**
     * Checks if the daily quota circuit is open for a WABA.
     */
    public boolean isDailyQuotaCircuitOpen(String wabaId) {
        return isCircuitOpen(WABA_DAILY_QUOTA_EXHAUSTED_PREFIX + wabaId);
    }

    /**
     * Opens the WABA daily quota circuit (80007 scope).
     * When any phone number under a WABA hits error code 80007,
     * this blocks ALL phone numbers under that WABA for 24 hours.
     */
    public void openDailyQuotaCircuit(String wabaId) {
        openCircuit(WABA_DAILY_QUOTA_EXHAUSTED_PREFIX + wabaId, 86400L); // 24 hours
    }

    /**
     * Checks if the burst/MPS circuit (130429 scope) for a specific
     * waba-phone-number-id.
     */
    public boolean isBurstLimitCircuitOpen(String wabaPhoneNumberId) {
        return isCircuitOpen(WABA_PHONE_NUMBER_RATE_LIMIT_PREFIX + wabaPhoneNumberId);
    }

    /**
     * Opens the burst/MPS circuit (130429 scope) for a specific
     * waba-phone-number-id.
     */
    public void openBurstLimitCircuit(String wabaPhoneNumberId, long retryAfterSeconds) {
        openCircuit(WABA_PHONE_NUMBER_RATE_LIMIT_PREFIX + wabaPhoneNumberId, retryAfterSeconds);
    }

    /**
     * Checks if the circuit is open (rate limited) for the given key.
     * <p>
     * Uses {@code hasKey()} to check key existence. When the TTL expires,
     * the key is auto-deleted by Redis, which effectively closes the circuit.
     */
    private boolean isCircuitOpen(String key) {
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
    private void openCircuit(String key, long keepCircuitOpenForSeconds) {
        try {
            redisTemplate.opsForValue().set(key, "OPEN", Duration.ofSeconds(keepCircuitOpenForSeconds));
            log.warn("Circuit OPENED for key : [{}]. Paused for {} seconds.", key, keepCircuitOpenForSeconds);
        } catch (Exception ex) {
            log.error("Failed to open circuit breaker for key : [{}]. | Exception occurred: {}", key, ex.getMessage());
        }
    }
}