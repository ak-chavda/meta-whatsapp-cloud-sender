package com.whatsapp.sender.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Campaign outbound batch event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OutboundBatchEvent(

        Integer campaignId,
        Integer batchId,
        String wabaPhoneNumberId, // Nullable, if provided strictly, send message from this WABA. Else rotate WABAs.
        String templateId, // Nullable, if provided strictly, send message with this Template. Else rotate Templates.
        List<String> targetPhoneNumbers,

        // These preserve fields are computed internally
        boolean preserveWaBaPhoneNumberId,
        boolean preserveTemplateId) {

    public boolean preserveWaBaPhoneNumberId() {
        return this.wabaPhoneNumberId() != null;
    }

    public boolean preserveTemplateId() {
        return this.templateId() != null;
    }
}