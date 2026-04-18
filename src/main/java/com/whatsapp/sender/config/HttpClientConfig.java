package com.whatsapp.sender.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Configures the Java 21 {@link HttpClient} used for WhatsApp Cloud API calls.
 * <p>
 * The client is backed by a Virtual Thread executor, meaning each HTTP call
 * runs on its own virtual thread — ideal for I/O-bound fan-out of 100+ concurrent
 * requests per batch without platform thread exhaustion.
 * <p>
 * Also exposes a shared {@link ExecutorService} for batch fan-out coordination.
 */
@Configuration
public class HttpClientConfig {

    @Value("${app.whatsapp.http-client.connect-timeout-ms:5000}")
    private int connectTimeoutMs;

    /**
     * Virtual Thread executor for fan-out concurrency.
     * <p>
     * Each task submitted here runs on a dedicated virtual thread.
     * No pool sizing, no queue contention, no thread starvation.
     */
    @Bean
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * HTTP/2-capable client backed by virtual threads.
     * <p>
     * HTTP/2 enables multiplexing multiple WhatsApp API requests over fewer TCP connections to {@code graph.facebook.com}.
     */
    @Bean
    public HttpClient httpClient(ExecutorService virtualThreadExecutor) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .executor(virtualThreadExecutor)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }
}