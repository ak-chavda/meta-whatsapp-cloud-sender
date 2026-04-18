package com.whatsapp.sender.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;

/**
 * Internal status result representing the outcome of a batch dispatch.
 * <p>
 * Used internally within the Sender Service pipeline for tracking
 * dispatch results and persisting to MongoDB. This is NOT published to
 * a Kafka status topic — only failure events are published externally.
 *
 * @param batchId              originating batch identifier for traceability
 * @param campaignId           parent campaign identifier
 * @param targetPhoneNumbers   list of target phone numbers processed in this result
 * @param isSendSuccessful     true if the API call returned HTTP 200/201, false otherwise
 * @param errorMessage         detailed error description
 * @param whatsappMessageId    message ID from Meta API (null on failure)
 * @param retryCount           current retry attempt count
 * @param timestamp            when the result was recorded
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
        Instant timestamp
) {}