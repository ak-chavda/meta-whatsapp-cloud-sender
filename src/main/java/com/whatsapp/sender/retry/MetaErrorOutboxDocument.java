package com.whatsapp.sender.retry;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB document for storing failed WhatsApp messages that require delayed
 * retries (Transactional Outbox Pattern).
 * <p>
 * Supports all retryable error types:
 * <ul>
 *   <li><strong>130429 (Burst/MPS Limit)</strong>: Per Phone Number ID,
 *       exponential backoff with jitter (seconds).</li>
 *   <li><strong>80007 (Daily Quota Limit)</strong>: Per WABA (all phone numbers
 *       share the pool), strict 24-hour retry.</li>
 *   <li><strong>5xx (Server Errors)</strong>: Transient server errors,
 *       short exponential backoff (60s, 120s, 300s).</li>
 * </ul>
 * <p>
 * Uses a composite index on {"status": 1, "error_code": 1, "retry_after": 1}
 * to prevent full table scans when the scheduler polls for ripe messages.
 */
@Document(collection = "meta_error_outbox")
@CompoundIndex(name = "status_error_retry_idx", def = "{'status': 1, 'error_code': 1, 'retry_after': 1}")
public record MetaErrorOutboxDocument(
        @Id 
        String id,

        @Field("campaign_id") 
        Integer campaignId,

        @Field("batch_id") 
        Integer batchId,

        @Field("waba_id") 
        String wabaId, // WABA ID — needed for Daily Quota (80007) scoped per WABA

        @Field("waba_phone_number_id") 
        String wabaPhoneNumberId, // Phone Number ID — needed for Burst Limit (130429) scoped per phone

        @Field("template_id") 
        String templateId,

        @Field("target_phone_numbers") 
        List<String> targetPhoneNumbers,

        @Field("error_code") 
        String errorCode, // e.g. "130429", "80007", "HTTP_500", "HTTP_503"

        @Field("error_message") 
        String errorMessage,

        @Field("status") 
        String status, // PENDING, PROCESSING

        @Field("retry_count") 
        int retryCount,

        @Field("retry_after") 
        Instant retryAfter,

        @Field("created_at") 
        Instant createdAt,

        @Field("worker_id")
        String workerId // Used for row-level locking with updateMany
) {}
