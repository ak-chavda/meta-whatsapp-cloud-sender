package com.whatsapp.sender.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;
import com.whatsapp.sender.dto.WhatsAppTemplateRequest;
import com.whatsapp.sender.dto.WhatsAppTemplateRequest.Template;
import com.whatsapp.sender.dto.WhatsappApiResponse;
import com.whatsapp.sender.util.Utils;

/**
 * Low-level HTTP client for the WhatsApp Cloud API (Meta Graph API).
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Construct the template-based message payload per Meta API spec using typed DTOs.</li>
 *   <li>Execute a synchronous HTTP POST and return a typed result.</li>
 *   <li>Map HTTP status codes to structured error codes without retrying.</li>
 * </ul>
 * <p>
 * This class is intentionally designed for synchronous, blocking calls because
 * concurrency is managed at the batch dispatcher level using Virtual Threads.
 * Each virtual thread runs one {@link #sendMessage} call — the blocking I/O
 * is exactly what virtual threads are optimized for.
 * <p>
 * Template details (name, language, components) are fetched from the Campaign
 * Service at runtime using the templateId from the campaign's quota configuration.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WhatsappApiClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Value("${app.whatsapp.api-version}")
    private String apiVersion;

    @Value("${app.whatsapp.base-url}")
    private String baseUrl;

    @Value("${app.whatsapp.http-client.request-timeout-ms}")
    private int requestTimeoutMs;

    /**
     * Result of a single WhatsApp API call.
     *
     * @param success           whether the API call returned HTTP 200
     * @param httpStatusCode    raw HTTP status code
     * @param errorCode         formatted error code (e.g., "HTTP_200", "HTTP_429")
     * @param whatsappMessageId message ID from Meta API (null on failure)
     * @param errorDetail       detailed error message on failure
     * @param retryAfterSeconds retry-after header value for 429 responses
     */
    public record SendResult(
            boolean success,
            int httpStatusCode,
            String errorCode,
            String whatsappMessageId,
            String errorDetail,
            Long retryAfterSeconds) {
    }

    /**
     * Sends a single template message to the WhatsApp Cloud API.
     * This is a blocking call, designed to run on a virtual thread.
     * @return structured result with status code mapping
     */
    public SendResult sendMessage(String wabaPhoneNumberId, String templateId, String accessToken, String targetPhoneNumber, Campaign campaign) {

        final String apiUrl = String.format("%s/%s/%s/messages", baseUrl, apiVersion, wabaPhoneNumberId);

        try {
            // Build the Meta API payload using typed DTOs
            final String payload = buildTemplatePayload(targetPhoneNumber, templateId, campaign);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(requestTimeoutMs))
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            log.debug("Sending message to [{}] via WaBa phone number ID [{}]", targetPhoneNumber, wabaPhoneNumberId);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            final int statusCode = response.statusCode();
            String errorCode = "HTTP_" + statusCode;

            if (statusCode == 200 || statusCode == 201) {
                // Parse response to extract message ID
                WhatsappApiResponse apiResponse = objectMapper.readValue(response.body(), WhatsappApiResponse.class);
                String messageId = apiResponse.extractMessageId();
                log.debug("Message sent successfully to [{}]. WhatsApp Message ID: {}", targetPhoneNumber, messageId);
                return new SendResult(true, statusCode, errorCode, messageId, null, null);

            } else {
                // Non-success HTTP status — do NOT retry, just map the code
                final String rawErrorBody = response.body();

                try {
                    com.whatsapp.sender.dto.MetaApiErrorResponse errorResponse = objectMapper.readValue(rawErrorBody, com.whatsapp.sender.dto.MetaApiErrorResponse.class);
                    if (errorResponse != null && errorResponse.error() != null) {
                        errorCode = "META_" + errorResponse.error().code();
                    }

                } catch (Exception e) {
                    // Ignore parse exception, fallback to HTTP_xxx
                }

                String errorBody = truncateErrorBody(rawErrorBody);
                log.warn("WhatsApp API returned HTTP {} for phone [{}]: {}", statusCode, targetPhoneNumber, errorBody);

                Long retryAfter = null;
                if (statusCode == 429) {
                    retryAfter = response.headers().firstValue("Retry-After")
                            .map(val -> {
                                try {
                                    return Long.parseLong(val);
                                } catch (NumberFormatException e) {
                                    return 60L;
                                }
                            })
                            .orElse(60L);
                }
                return new SendResult(false, statusCode, errorCode, null, errorBody, retryAfter);
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted while sending to [{}]", targetPhoneNumber);
            return new SendResult(false, 0, "INTERRUPTED", null, "Thread interrupted: " + ex.getMessage(), null);

        } catch (Exception ex) {
            log.error("Exception sending message to [{}]: {}", targetPhoneNumber, ex.getMessage(), ex);
            return new SendResult(false, 0, "CLIENT_ERROR", null, "Client exception: " + ex.getMessage(), null);
        }
    }

    private String buildTemplatePayload(String targetPhoneNumber, String templateId, Campaign campaign) throws Exception {
        TemplateDetail templateDetail = Utils.findTemplateDetail(campaign, templateId);
        final Template template = new Template(templateDetail.name(), templateDetail.language(), templateDetail.components());
        final WhatsAppTemplateRequest requestPayload = new WhatsAppTemplateRequest(targetPhoneNumber, template);
        return objectMapper.writeValueAsString(requestPayload);
    }

    /**
     * Truncates error response body to prevent log pollution from large API error responses.
     */
    private String truncateErrorBody(String body) {
        if (body == null)
            return "No response body";
        return body.length() > 500 ? body.substring(0, 500) + "...[truncated]" : body;
    }
}