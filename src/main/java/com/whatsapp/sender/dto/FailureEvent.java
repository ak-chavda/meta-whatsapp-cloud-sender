package com.whatsapp.sender.dto;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Failure event published to the {@code whatsapp-failures-retry} topic
 * by the MetaErrorSchedulers after ripe outbox documents are ready for retry.
 * <p>
 * Carries the full context needed by the Retry Worker to re-execute the
 * WhatsApp API call without re-fetching campaign details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailureEvent(

        Integer batchId,
        Integer campaignId,

        String wabaId,
        String wabaPhoneNumberId,
        String templateId,

        List<String> targetPhoneNumbers,
        String errorCode,
        String errorMessage,
        int currentRetryCount,
        Instant timestamp) {
}