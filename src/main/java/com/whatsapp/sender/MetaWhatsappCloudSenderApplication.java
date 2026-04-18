package com.whatsapp.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Meta WhatsApp Cloud Sender Service.
 * <p>
 * Stateless event-driven worker that consumes message batches from Kafka,
 * fans out HTTP calls to the WhatsApp Cloud API using Virtual Threads,
 * and persists dispatch results to MongoDB.
 * <p>
 * Virtual threads are enabled globally via {@code spring.threads.virtual.enabled=true}.
 * <p>
 * Scheduling is enabled for the {@link com.whatsapp.sender.retry.MetaErrorSchedulers}
 * which polls the MongoDB outbox for ripe retryable messages and pushes them
 * to the Kafka retry topic.
 */
@EnableScheduling
@SpringBootApplication
public class MetaWhatsappCloudSenderApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetaWhatsappCloudSenderApplication.class, args);
    }
}
