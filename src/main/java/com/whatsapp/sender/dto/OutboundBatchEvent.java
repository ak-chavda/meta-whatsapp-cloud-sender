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
        String wabaPhoneNumberId, // Nullable, if provided strictly, send message from this Waba. Else rotate Wabas.
        String templateId, // Nullable, if provided strictly, send message with this Template. Else rotate Templates.
        List<String> targetPhoneNumbers) {
}