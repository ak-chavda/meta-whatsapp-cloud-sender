package com.whatsapp.sender.repository;

import com.whatsapp.sender.dao.MessageDispatchDocument;
import com.whatsapp.sender.dto.MessageStatusResultEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Asynchronous, non-blocking MongoDB writer for message dispatch state.
 * <p>
 * Persists the dispatch outcome (success/failure) along with the batch of
 * target phone numbers. Documents are grouped by error code within a batch,
 * significantly reducing document count for large batches.
 * <p>
 * Writes are submitted to the virtual thread executor to avoid blocking
 * the dispatch pipeline. MongoDB write failures are logged but do NOT
 * propagate — MongoDB is the persistent audit log.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageStateRepository {

    private final MongoTemplate mongoTemplate;
    private final ExecutorService virtualThreadExecutor;

    /**
     * Asynchronously persists a message dispatch result to MongoDB.
     * <p>
     * Runs on a virtual thread to avoid blocking the batch dispatcher.
     *
     * @param event             the status result event containing dispatch outcome
     * @param wabaPhoneNumberId the WaBa phone number ID used for this dispatch
     */
    public void saveDispatchResult(MessageStatusResultEvent event, String wabaPhoneNumberId, String templateId) {
        virtualThreadExecutor.submit(() -> {
            try {
                MessageDispatchDocument document = new MessageDispatchDocument(
                        UUID.randomUUID().toString(),
                        event.campaignId(),
                        event.batchId(),
                        event.targetPhoneNumbers(),
                        event.whatsappMessageId(),
                        event.isSendSuccessful(),
                        event.errorCode(),
                        wabaPhoneNumberId,
                        templateId,
                        Instant.now()
                );

                mongoTemplate.save(document);
                log.debug("Persisted dispatch result for {} phone numbers, success: {}", event.targetPhoneNumbers().size(), event.isSendSuccessful());

            } catch (Exception e) {
                log.error("Failed to persist dispatch result for batch [{}]: {}", event.batchId(), e.getMessage(), e);
            }
        });
    }
}