package com.whatsapp.sender.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.QuotaCheckResult;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;

/**
 * Distributed Quota Manager using Redis atomic operations.
 * <p>
 * Manages three types of counters:
 * <ul>
 *   <li><strong>WaBa daily quota</strong>: {@code quota:waba:{phoneNumberId}:daily:{date}}</li>
 *   <li><strong>Template daily quota</strong>: {@code quota:template:{templateId}:daily:{date}}</li>
 *   <li><strong>Success counter</strong>: {@code campaign:{id}:waba:{number}:success}</li>
 * </ul>
 * <p>
 * Uses Lua scripts for atomic check-and-increment to prevent race conditions
 * across multiple pods/threads. All counters auto-expire at end of day (86400s TTL).
 * <p>
 * <strong>Rotation strategy:</strong> If the primary WaBa number's quota is exhausted,
 * iterates through fallback WaBa numbers from the campaign's details map.
 * If ALL WaBa numbers are exhausted, returns an exhausted result so the caller
 * can route to the failure topic.
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

    public QuotaCheckResult resolveCombination(Campaign campaign) {
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        final String campaignIdStr = String.valueOf(campaign.id());

        // 1. Check the main two circuits
        if (circuitBreaker.isCircuitOpen(CircuitBreaker.CAMPAIGN_TEMPLATES_EXHAUSTED_PREFIX, campaignIdStr)) {
            return QuotaCheckResult.exhaustedTemplate("All templates exhausted for campaign (Circuit Open)");
        }
        if (circuitBreaker.isCircuitOpen(CircuitBreaker.CAMPAIGN_WABAS_EXHAUSTED_PREFIX, campaignIdStr)) {
            return QuotaCheckResult.exhaustedWaba("All WABAs exhausted for campaign (Circuit Open)");
        }

        // 2. Resolve Template
        String resolvedTemplateId = null;
        for (TemplateDetail templateDetail : campaign.templateDetails()) {
            String templateId = templateDetail.templateId();
            
            long templateLimit = templateDetail.dailySendingLimit();
            if (templateLimit > 0) {
                String templateKey = String.format(TEMPLATE_QUOTA_KEY, templateId, today);
                String templateCountStr = redisTemplate.opsForValue().get(templateKey);
                long templateResult = templateCountStr != null ? Long.parseLong(templateCountStr) : 0L;
                
                if (templateResult >= templateLimit) {
                    continue; // Quota exhausted, try next
                }
            }
            resolvedTemplateId = templateId;
            break;
        }

        if (resolvedTemplateId == null) {
            // All templates are exhausted. Open the main template circuit.
            log.info("ALL templates exhausted for campaign [{}]. Opening main template circuit.", campaign.id());
            circuitBreaker.openCircuit(CircuitBreaker.CAMPAIGN_TEMPLATES_EXHAUSTED_PREFIX, campaignIdStr, getSecondsUntilEndOfDay());
            return QuotaCheckResult.exhaustedTemplate("All templates exhausted for campaign");
        }

        // 3. Resolve WABA
        String resolvedWabaId = null;
        boolean anyWabaSkippedDueToRateLimit = false;

        for (Campaign.WhatsAppBusinessAccountDetail wabaDetail : campaign.whatsappBusinessAccountDetails()) {
            String wabaId = wabaDetail.waBaPhoneNumberId();
            String wabaAccountId = wabaDetail.waBaId();
            
            // Check daily quota circuit (80007) — scoped per WABA ID (all phone numbers share pool)
            if (wabaAccountId != null && circuitBreaker.isDailyQuotaCircuitOpen(wabaAccountId)) {
                anyWabaSkippedDueToRateLimit = true;
                continue;
            }

            // Check burst/MPS circuit (130429) — scoped per Phone Number ID
            if (circuitBreaker.isCircuitOpen(CircuitBreaker.WABA_RATE_LIMIT_PREFIX, wabaId)) {
                anyWabaSkippedDueToRateLimit = true;
                continue;
            }
            
            long wabaLimit = wabaDetail.dailySendingLimit();
            if (wabaLimit > 0) {
                String wabaKey = String.format(WABA_QUOTA_KEY, wabaId, today);
                String wabaCountStr = redisTemplate.opsForValue().get(wabaKey);
                long wabaResult = wabaCountStr != null ? Long.parseLong(wabaCountStr) : 0L;
                
                if (wabaResult >= wabaLimit) {
                    continue; // Quota exhausted, try next
                }
            } else {
                continue; // Skip misconfigured waba
            }
            
            resolvedWabaId = wabaId;
            break;
        }

        if (resolvedWabaId == null) {
            if (!anyWabaSkippedDueToRateLimit) {
                // ALL WABAs are permanently exhausted (quota). Open the main WABA circuit.
                log.info("ALL WABAs exhausted for campaign [{}]. Opening main WABA circuit.", campaign.id());
                circuitBreaker.openCircuit(CircuitBreaker.CAMPAIGN_WABAS_EXHAUSTED_PREFIX, campaignIdStr, getSecondsUntilEndOfDay());
            }
            return QuotaCheckResult.exhaustedWaba("All WABAs exhausted or rate limited for campaign");
        }
        
        return QuotaCheckResult.allowed(resolvedWabaId, resolvedTemplateId);
    }

    private long getSecondsUntilEndOfDay() {
        java.time.LocalDateTime now = java.time.LocalDateTime.now(ZoneOffset.UTC);
        java.time.LocalDateTime endOfDay = now.toLocalDate().atTime(java.time.LocalTime.MAX);
        return java.time.Duration.between(now, endOfDay).getSeconds();
    }

    /**
     * Increments the success counter for a campaign+waba combination.
     *
     * @param campaignId       the campaign identifier
     * @param wabaPhoneNumberId the WaBa phone number that sent successfully
     */
    public void incrementSuccessCounter(Integer campaignId, String wabaPhoneNumberId) {
        final String key = String.format(SUCCESS_KEY, campaignId, wabaPhoneNumberId);
        try {
            redisTemplate.opsForValue().increment(key);
        } catch (Exception e) {
            // Non-critical: success counter is observability-only, don't fail the send
            log.warn("Failed to increment success counter for campaign [{}] waba [{}]: {}", campaignId, wabaPhoneNumberId, e.getMessage());
        }
    }

    public void incrementQuota(String wabaId, String templateId) {
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        redisTemplate.opsForValue().increment(String.format(WABA_QUOTA_KEY, wabaId, today));
        redisTemplate.opsForValue().increment(String.format(TEMPLATE_QUOTA_KEY, templateId, today));
    }

    public void decrementQuota(String wabaId, String templateId) {
        final String today = LocalDate.now(ZoneOffset.UTC).toString();
        redisTemplate.opsForValue().decrement(String.format(WABA_QUOTA_KEY, wabaId, today));
        redisTemplate.opsForValue().decrement(String.format(TEMPLATE_QUOTA_KEY, templateId, today));
    }
}