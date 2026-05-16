// package com.whatsapp.sender.dto;

// import java.util.List;

// import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// /**
//  * Failure event published to the {@code whatsapp-failures-retry} topic
//  * by the MetaErrorSchedulers after ripe outbox documents are ready for retry.
//  * <p>
//  * Carries the full context needed by the Retry Worker to re-execute the
//  * WhatsApp API call without re-fetching campaign details.
//  */
// @JsonIgnoreProperties(ignoreUnknown = true)
// public record RetryEvent(

//         Integer batchId,
//         Integer campaignId,
//         List<String> targetPhoneNumbers) {
// }