package com.whatsapp.sender.dao;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Source of truth for the message dispatch status.
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "campaign_message_dispatch_log")
@CompoundIndex(name = "campaign_batch_idx", def = "{'campaign_id': 1, 'batch_id': 1}")
public class MessageDispatchDocument {

    @Id
    private String id;

    @Field("campaign_id")
    private Integer campaignId;

    @Field("batch_id")
    private Integer batchId;

    /**
     * This number will only be saved in DB incase of succeess / final failure (after all attempts exhausted).
     */
    @Field("whats_app_phone_number_id")
    private String whatsAppPhoneNumberId;

    /**
     * This template-ID will only be saved in DB incase of succeess / final failure (after all attempts exhausted).
     */
    @Field("whats_app_template_id")
    private String whatsAppTemplateId;

    @Indexed
    @Field("mobile")
    private String mobile;

    /**
     * This messageId will only be saved in DB incase of succeess.
     */
    @Field("whats_app_message_id")
    private String whatsAppMessageId;

    /**
     * final status of the message (success, failed, retry).
     */
    @Field("status")
    private Status status;

    /**
     * error code for the final failure.
     */
    @Field("error_code")
    private String errorCode;

    /**
     * error message for the final failure.
     */
    @Field("error_message")
    private String errorMessage;

    /**
     * This timestamp will be overridden for every attempt (irrespective of success / failure).
     */
    @Field("attempt_at")
    private Instant attemptAt;

    @Field("retry_after") 
    Instant retryAfter;

    /**
     * Used for row-level locking, so the same record is not picked up by multiple workers/pods.
     */
    @Field("worker_id")
    String workerId;

    /**
     * This is counter variable, to store number of attempts. 
     * It will be incremented and updated for every attempt.
     */
    @Field("attempts")
    private Integer attempts = 0;

    /**
     * This list will store logs of every attempt.
     * Only retryable failures will be considered for this logging.
     * (for the final one it will be visible in root object).
     */
    @Field("attempt_logs")
    private List<AttemptLog> attemptLogs = new ArrayList<>();

    public enum Status {
        SUCCESS, FAILED, RETRY
    }

    @Getter
    @Setter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class AttemptLog {

        @Field("whats_app_phone_number_id")
        private String whatsAppPhoneNumberId;

        @Field("whats_app_template_id")
        private String whatsAppTemplateId;

        @Field("error_code")
        private String errorCode;

        @Field("error_message")
        private String errorMessage;

        @Field("attempt_at")
        private Instant attemptAt;
    }
}