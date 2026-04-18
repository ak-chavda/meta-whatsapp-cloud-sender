package com.whatsapp.sender.retry;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data MongoDB repository for MetaErrorOutboxDocument.
 * 
 * Custom queries for row-level locking (updateMany / findAndModify) 
 * are handled in MetaErrorOutboxService using MongoTemplate,
 * as Spring Data interfaces do not natively support atomic
 * bulk update-and-return in a single method.
 */
@Repository
public interface MetaErrorOutboxRepository extends MongoRepository<MetaErrorOutboxDocument, String> {
}
