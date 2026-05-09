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
        List<String> targetPhoneNumbers,
        boolean isSendSuccessful,
        String errorCode,
        String errorMessage,
        String whatsappMessageId,
        int retryCount,
        Instant timestamp) {
}