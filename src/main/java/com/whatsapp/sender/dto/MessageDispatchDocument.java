package com.whatsapp.sender.dto;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB document tracking the send status of WhatsApp messages per batch.
 * <p>
 * Written asynchronously by the Sender Service after each API call completes.
 * Provides the source of truth for message delivery state.
 * <p>
 * Space-efficient: groups target phone numbers by outcome within the same batch.
 * For example, if a batch of 100 targets has 80 successes and 20 failures with
 * the same error code, only 2 documents are created instead of 100.
 * <p>
 * Collection: {@code message_dispatch_log}
 */
@Document(collection = "message_dispatch_log")
@CompoundIndex(name = "campaign_batch_idx", def = "{'campaign_id': 1, 'batch_id': 1}")
public record MessageDispatchDocument(

        @Id
        String id,

        @Field("campaign_id")
        Integer campaignId,

        @Field("batch_id")
        Integer batchId,

        @Indexed
        @Field("target_phone_numbers")
        List<String> targetPhoneNumbers,

        @Field("wa_mesg_id")
        String whatsappMessageId,

        @Field("is_send_successful")
        boolean isSendSuccessful,

        @Field("error_code")
        String errorCode,

        @Field("waba_phone_number_id")
        String wabaPhoneNumberId,

        @Field("template_id")
        String templateId,

        @Field("dispatched_at")
        Instant dispatchedAt
) {}