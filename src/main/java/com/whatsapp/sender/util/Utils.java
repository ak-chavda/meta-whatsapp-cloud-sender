package com.whatsapp.sender.util;

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
}