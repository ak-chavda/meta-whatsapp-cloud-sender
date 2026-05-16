package com.whatsapp.sender.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;
import com.whatsapp.sender.dto.Campaign.WabaNumberDetail;
import com.whatsapp.sender.service.WhatsappApiClient;

public class Utils {

    private Utils() {
    }

    /**
     * Resolves the error code for outbox storage.
     * Strips "META_" prefix for Meta API errors, keeps "HTTP_xxx" for HTTP errors.
     */
    public static String resolveErrorCode(WhatsappApiClient.SendResult sendResult) {
        String errorCode = sendResult.errorCode();
        if (errorCode != null && errorCode.startsWith("META_")) {
            return errorCode.substring(5); // "META_130429" → "130429"
        }
        return errorCode; // "HTTP_500" stays as-is
    }

    /**
     * Determines if a send result is retryable.
     * Retryable: 429, 5xx, META_130429 (burst/MPS), META_80007 (daily quota).
     */
    public static boolean isRetryable(WhatsappApiClient.SendResult sendResult) {
        if (sendResult.httpStatusCode() == 429 || sendResult.httpStatusCode() >= 500) {
            return true;
        }
        String errorCode = sendResult.errorCode();
        if (errorCode != null) {
            return errorCode.equals("META_130429") || errorCode.equals("META_80007");
        }
        return false;
    }

    public static TemplateDetail findTemplateDetail(Campaign campaign, String templateId) {
        if (campaign.templates() == null)
            return null;
        return campaign.templates().stream()
                .filter(t -> templateId.equals(t.templateId()))
                .findFirst()
                .orElse(null);
    }

    public static WabaNumberDetail findWabaDetail(Campaign campaign, String wabaPhoneNumberId) {
        if (campaign.wabaNumbers() == null)
            return null;
        return campaign.wabaNumbers().stream()
                .filter(w -> wabaPhoneNumberId.equals(w.wabaPhoneNumberId()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Calculates the retry-after timestamp based on error type and retry count.
     * <p>
     * <ul>
     *   <li><strong>80007</strong>: Strict 24 hours (daily quota is a hard stop)</li>
     *   <li><strong>130429</strong>: Exponential backoff (5s, 10s, 20s, 40s, 60s cap)</li>
     *   <li><strong>5xx / HTTP_5xx</strong>: Exponential backoff (60s, 120s, 300s cap)</li>
     *   <li><strong>Fallback</strong>: 60 seconds</li>
     * </ul>
     */
    public static Instant calculateRetryAfter(String errorCode, int retryCount) {
        if ("80007".equals(errorCode)) {
            // Daily Quota Limit: Hard stop. Strict 24 hours.
            return Instant.now().plus(24, ChronoUnit.HOURS);
        }

        if ("130429".equals(errorCode)) {
            // Burst Limit: Exponential backoff (5s, 10s, 20s, 40s, 60s cap)
            int baseDelaySeconds = 5 * (int) Math.pow(2, retryCount);
            if (baseDelaySeconds > 60) {
                baseDelaySeconds = 60;
            }
            return Instant.now().plus(baseDelaySeconds, ChronoUnit.SECONDS);
        }

        if (errorCode != null && errorCode.startsWith("HTTP_5")) {
            // 5xx Server Error: Exponential backoff (60s, 120s, 300s cap)
            int baseDelaySeconds = 60 * (int) Math.pow(2, retryCount);
            if (baseDelaySeconds > 300) {
                baseDelaySeconds = 300;
            }
            return Instant.now().plus(baseDelaySeconds, ChronoUnit.SECONDS);
        }

        // Fallback: 60 seconds
        return Instant.now().plus(60, ChronoUnit.SECONDS);
    }
}