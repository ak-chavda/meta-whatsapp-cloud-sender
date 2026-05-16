// package com.whatsapp.sender.retry;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.whatsapp.sender.dto.FailureEvent;
// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.stereotype.Service;

// /**
//  * Dead Letter Queue service for messages that have exhausted all retry attempts.
//  * <p>
//  * Publishes the final failure payload with the complete error trail to the
//  * {@code whatsapp-failures-dlq} topic. These messages represent permanent
//  * delivery failures that require manual investigation or operational intervention.
//  * <p>
//  * DLQ consumers (separate monitoring service) can:
//  * <ul>
//  *   <li>Alert ops teams on high DLQ volume</li>
//  *   <li>Persist to a permanent failure audit log</li>
//  *   <li>Provide a UI for manual replay of specific messages</li>
//  * </ul>
//  */
// @Slf4j
// @Service
// @RequiredArgsConstructor
// public class DlqService {

//     private final KafkaTemplate<String, String> kafkaTemplate;
//     private final ObjectMapper objectMapper;

//     @Value("${app.kafka.topics.campaign-failures-dlq}")
//     private String dlqTopic;

//     /**
//      * Publishes a failure event to the DLQ topic.
//      */
//     public void routeToDlq(FailureEvent failureEvent, String reason) {
//         try {
//             String json = objectMapper.writeValueAsString(failureEvent);
//             String key = String.valueOf(failureEvent.campaignId());

//             kafkaTemplate.send(dlqTopic, key, json)
//                     .whenComplete((result, e) -> {
//                         if (e != null) {
//                             log.error("!!! CRITICAL: Failed to publish to DLQ for campaign [{}] with {} targets: {}. MESSAGE MAY BE LOST!", 
//                                 failureEvent.campaignId(), failureEvent.targetPhoneNumbers().size(), e.getMessage());

//                         } else {
//                             log.warn("!!! Published to DLQ: campaign [{}] with {} targets. Reason: {}. Partition [{}] Offset [{}]",
//                                     failureEvent.campaignId(), failureEvent.targetPhoneNumbers().size(), reason,
//                                     result.getRecordMetadata().partition(),
//                                     result.getRecordMetadata().offset());
//                         }
//                     });

//         } catch (Exception e) {
//             log.error("CRITICAL: Failed to serialize/publish DLQ event for campaign [{}]: {}. MESSAGE MAY BE LOST!", failureEvent.campaignId(), e.getMessage());
//         }
//     }
// }