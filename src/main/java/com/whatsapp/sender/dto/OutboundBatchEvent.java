package com.whatsapp.sender.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Inbound Kafka payload representing a batch of message targets
 * consumed from topic {@code campaign-outbound-batch}.
 * <p>
 * Minimal payload — only the essential identifiers needed to initiate dispatch.
 * All enrichment (template details, WaBa details, access tokens) is resolved
 * at runtime from the Campaign Service via {@code CampaignCacheService}.
 *
 * @param campaignId         parent campaign identifier
 * @param batchId            unique batch identifier within the campaign
 * @param targetPhoneNumbers list of E.164 formatted phone numbers to message
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OutboundBatchEvent(

        Integer campaignId,
        Integer batchId,
        List<String> targetPhoneNumbers
) {}