package com.whatsapp.sender.service;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.whatsapp.sender.dao.MessageDispatchDocument;
import com.whatsapp.sender.dao.MessageDispatchDocument.Status;
import com.whatsapp.sender.dto.OutboundBatchEvent;
import com.whatsapp.sender.repository.MessageDispatchRepository;
import com.whatsapp.sender.util.Utils;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageStateService {

    private final MessageDispatchRepository messageDispatchRepository;
    private final MongoTemplate mongoTemplate;

    /**
     * Create non-retryable failures for mobiles in batchEvent.
     */
    public void insertBatchFailures(OutboundBatchEvent batchEvent, String whatsAppPhoneNumberId, String whatsAppTemplateId, String errorCode, String errorMessage) {
        List<MessageDispatchDocument> documents = batchEvent.targetPhoneNumbers().stream()
                .map(targetPhoneNumber -> new MessageDispatchDocument(
                        null,
                        batchEvent.campaignId(),
                        batchEvent.batchId(),
                        whatsAppPhoneNumberId,
                        whatsAppTemplateId,
                        targetPhoneNumber,
                        null,
                        Status.FAILED,
                        errorCode,
                        errorMessage,
                        Instant.now(),
                        null,
                        null,
                        0,
                        new ArrayList<>()))
                .toList();

        messageDispatchRepository.saveAll(documents);
        log.debug("Created {} new documents for campaign-id: {} and batch-id: {}", documents.size(), batchEvent.campaignId(), batchEvent.batchId());
    }

    /**
     * Update non-retryable failures for mobiles in batchEvent.
     */
    public void updateBatchFailures(OutboundBatchEvent batchEvent, String whatsAppPhoneNumberId, String whatsAppTemplateId, String errorCode, String errorMessage) {

        List<MessageDispatchDocument> documents = messageDispatchRepository.findByCampaignIdAndMobileIn(batchEvent.campaignId(), batchEvent.targetPhoneNumbers());

        final Instant currentTimeStamp = Instant.now();
        documents.stream().forEach(document -> {
            document.setWhatsAppPhoneNumberId(whatsAppPhoneNumberId);
            document.setWhatsAppTemplateId(whatsAppTemplateId);
            document.setStatus(Status.FAILED);
            document.setErrorCode(errorCode);
            document.setErrorMessage(errorMessage);
            document.setAttemptAt(currentTimeStamp);
            document.setWorkerId(null); // resetting the worker id as the message is processed.
        });
        messageDispatchRepository.saveAll(documents);
        log.debug("Created {} new documents for campaign-id: {} and batch-id: {}", documents.size(), batchEvent.campaignId(), batchEvent.batchId());
    }

    /**
     * Update success for the mobile.
     */
    public void updateSuccessForMobile(Integer campaignId, Integer batchId, String mobile, String whatsAppPhoneNumberId, String whatsAppTemplateId, String whatsappMessageId) {
        MessageDispatchDocument document = messageDispatchRepository.findByCampaignIdAndMobile(campaignId, mobile).orElseGet(() -> {
            MessageDispatchDocument tempDoc = new MessageDispatchDocument();
            tempDoc.setCampaignId(campaignId);
            tempDoc.setBatchId(batchId);
            tempDoc.setMobile(mobile);
            return tempDoc;
        });

        document.setWhatsAppPhoneNumberId(whatsAppPhoneNumberId);
        document.setWhatsAppTemplateId(whatsAppTemplateId);
        document.setWhatsAppMessageId(whatsappMessageId);
        document.setStatus(Status.SUCCESS);
        document.setAttemptAt(Instant.now());
        document.setAttempts(document.getAttempts() + 1);
        document.setWorkerId(null); // resetting the worker id as the message is processed.

        log.info("Updated document for campaign-id: {} and mobile: {} | marked as SUCCESS.", campaignId, mobile);
        messageDispatchRepository.save(document);
    }

    /**
     * Update Retryable Failure for the mobile.
     */
    public void updateRetryableFailureForMobile(Integer campaignId, Integer batchId, String mobile, String whatsAppPhoneNumberId, String whatsAppTemplateId, String errorCode, String errorMessage) {
        MessageDispatchDocument document = messageDispatchRepository.findByCampaignIdAndMobile(campaignId, mobile).orElseGet(() -> {
            MessageDispatchDocument tempDoc = new MessageDispatchDocument();
            tempDoc.setCampaignId(campaignId);
            tempDoc.setBatchId(batchId);
            tempDoc.setMobile(mobile);
            return tempDoc;
        });

        document.setWhatsAppPhoneNumberId(whatsAppPhoneNumberId);
        document.setWhatsAppTemplateId(whatsAppTemplateId);
        document.setStatus(Status.RETRY);
        document.setAttempts(document.getAttempts() + 1);
        document.setRetryAfter(Utils.calculateRetryAfter(errorCode, document.getAttempts()));

        final Instant currentTimeStamp = Instant.now();
        document.setAttemptAt(currentTimeStamp);
        MessageDispatchDocument.AttemptLog aLog = new MessageDispatchDocument.AttemptLog(whatsAppPhoneNumberId, whatsAppTemplateId, errorCode, errorMessage, currentTimeStamp);
        document.getAttemptLogs().add(aLog);
        document.setWorkerId(null); // resetting the worker id as the message is processed.

        log.info("Updated document for campaign-id: {} and mobile: {} | marked as RETRY.", campaignId, mobile);
        messageDispatchRepository.save(document);
    }

    /**
     * Update Failure for the mobile.
     */
    public void updateFailureForMobile(Integer campaignId, Integer batchId, String mobile, String whatsAppPhoneNumberId, String whatsAppTemplateId, String errorCode, String errorMessage) {
        MessageDispatchDocument document = messageDispatchRepository.findByCampaignIdAndMobile(campaignId, mobile).orElseGet(() -> {
            MessageDispatchDocument tempDoc = new MessageDispatchDocument();
            tempDoc.setCampaignId(campaignId);
            tempDoc.setBatchId(batchId);
            tempDoc.setMobile(mobile);
            return tempDoc;
        });

        document.setWhatsAppPhoneNumberId(whatsAppPhoneNumberId);
        document.setWhatsAppTemplateId(whatsAppTemplateId);
        document.setStatus(Status.FAILED);
        document.setErrorCode(errorCode);
        document.setErrorMessage(errorMessage);
        document.setAttemptAt(Instant.now());
        document.setAttempts(document.getAttempts() + 1);
        document.setWorkerId(null); // resetting the worker id as the message is processed.

        log.info("Updated document for campaign-id: {} and mobile: {} | marked as FAILED.", campaignId, mobile);
        messageDispatchRepository.save(document);
    }

    /**
     * Atomically locks ripe messages for a given error code pattern and returns them.
     * Uses atomic updateMany to assign a unique worker ID, then queries by that ID.
     * 
     * Once the worker-id is assigned, it will be reset once the message is processed (Success/Retry/Failed).
     * So, if the worker fails to process the message, it will be picked up by another worker in the next run.
     * 
     * The order of processing is based on the retry_after field.
     */
    public List<MessageDispatchDocument> fetchAndLockRipeMessages(String errorCodePattern, boolean is5xx, int limit) {
        Criteria criteria = Criteria.where("status").is(MessageDispatchDocument.Status.RETRY).and("retry_after").lte(Instant.now());
        if (is5xx) {
            criteria = criteria.and("error_code").regex("^HTTP_5");
        } else {
            criteria = criteria.and("error_code").is(errorCodePattern);
        }

        final String workerId = UUID.randomUUID().toString();
        Query lockQuery = new Query(criteria).limit(limit);
        Update lockUpdate = new Update().set("worker_id", workerId);

        // Perform atomic updateMany (row-level locking)
        mongoTemplate.updateMulti(lockQuery, lockUpdate, MessageDispatchDocument.class);

        // Fetch the locked documents
        Query fetchQuery = new Query(Criteria.where("worker_id").is(workerId));
        return mongoTemplate.find(fetchQuery, MessageDispatchDocument.class);
    }
}