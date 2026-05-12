package com.whatsapp.sender.dto;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Internal status result representing the outcome of a batch dispatch.
 * <p>
 * Used internally within the Sender Service pipeline for tracking
 * dispatch results and persisting to MongoDB. This is NOT published to
 * a Kafka status topic — only failure events are published externally.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MessageStatusResultEvent(

        Integer batchId,
        Integer campaignId,
        String wabaId,
        String wabaPhoneNumberId,
        String templateId,
        List<String> targetPhoneNumbers,

        boolean isSendSuccessful,
        String whatsappMessageId,

        String errorCode,
        String errorMessage,

        int retryCount,
        Instant timestamp) {

    // Create Fail status event
    public static MessageStatusResultEvent createFailedStatusEvent(OutboundBatchEvent batch, String wabaId, List<String> targetPhoneNumbers, String errorCode, String errorMessage, int retryCount) {
        return new MessageStatusResultEvent(
                batch.batchId(),
                batch.campaignId(),
                wabaId,
                batch.preserveWaBaPhoneNumberId() ? batch.wabaPhoneNumberId() : null,
                batch.preserveTemplateId() ? batch.templateId() : null,
                targetPhoneNumbers,
                false,
                null,
                errorCode,
                errorMessage,
                retryCount,
                Instant.now());
    }

    // Create Success status event
    public static MessageStatusResultEvent createSuccessStatusEvent(OutboundBatchEvent batch, String wabaId, List<String> targetPhoneNumbers, String whatsappMessageId, int retryCount) {
        return new MessageStatusResultEvent(
                batch.batchId(),
                batch.campaignId(),
                wabaId,
                batch.preserveWaBaPhoneNumberId() ? batch.wabaPhoneNumberId() : null,
                batch.preserveTemplateId() ? batch.templateId() : null,
                targetPhoneNumbers,
                true,
                whatsappMessageId,
                null,
                null,
                retryCount,
                Instant.now());
    }
}