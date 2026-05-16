// package com.whatsapp.sender.dto;

// import java.time.Instant;
// import java.util.List;

// import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
// import com.whatsapp.sender.dao.MessageDispatchDocument;

// /**
//  * Internal status result representing the outcome of a batch dispatch.
//  * <p>
//  * Used internally within the Sender Service pipeline for tracking
//  * dispatch results and persisting to MongoDB. This is NOT published to
//  * a Kafka status topic — only failure events are published externally.
//  */
// @JsonIgnoreProperties(ignoreUnknown = true)
// public record MessageStatusResultEvent(

//         Integer campaignId,
//         Integer batchId,
//         String wabaId,
//         String wabaPhoneNumberId,
//         String templateId,
//         String targetPhoneNumber,

//         String status,
//         String whatsappMessageId,

//         String errorCode,
//         String errorMessage,

//         int attempts,
//         Instant timestamp) {

//     // Create Fail status event
//     public static MessageStatusResultEvent createFailedStatusEvent(OutboundBatchEvent batch, String wabaId, List<String> targetPhoneNumbers, String errorCode, String errorMessage, int attempt) {
//         return new MessageStatusResultEvent(
//                 batch.campaignId(),
//                 batch.batchId(),
//                 wabaId,
//                 null,
//                 null,
//                 targetPhoneNumbers,
//                 false,
//                 null,
//                 errorCode,
//                 errorMessage,
//                 attempt,
//                 Instant.now());
//     }

//     // Create Success status event
//     public static MessageStatusResultEvent createSuccessStatusEvent(OutboundBatchEvent batch, String wabaId, String wabaPhoneNumberId, String templateId, List<String> targetPhoneNumbers, String whatsappMessageId, int attempt) {
//         return new MessageStatusResultEvent(
//                 batch.campaignId(),
//                 batch.batchId(),
//                 wabaId,
//                 wabaPhoneNumberId,
//                 templateId,
//                 targetPhoneNumbers,
//                 true,
//                 whatsappMessageId,
//                 null,
//                 null,
//                 attempt,
//                 Instant.now());
//     }
// }