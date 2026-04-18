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
 *
 * @param campaignId                   parent campaign identifier
 * @param batchId                      originating batch identifier
 * @param wabaId                       the WABA ID (for 80007 daily quota scope)
 * @param wabaPhoneNumberId            the WaBa phone number ID used for sending
 * @param templateId                   the template ID used for this message
 * @param targetPhoneNumbers           list of phone numbers that failed in this batch
 * @param errorCode                    error code (e.g., "META_130429", "META_80007", "HTTP_500")
 * @param errorMessage                 detailed error description
 * @param currentRetryCount            how many times these targets have been retried so far
 * @param timestamp                    when the failure occurred
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailureEvent(

        Integer campaignId,
        Integer batchId,

        String wabaId,
        String wabaPhoneNumberId,
        String templateId,

        List<String> targetPhoneNumbers,
        String errorCode,
        String errorMessage,
        int currentRetryCount,
        Instant timestamp
) {}