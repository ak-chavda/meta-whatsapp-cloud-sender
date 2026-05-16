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
        List<String> targetPhoneNumbers) {
}