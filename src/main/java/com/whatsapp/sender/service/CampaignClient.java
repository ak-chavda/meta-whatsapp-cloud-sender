package com.whatsapp.sender.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.Campaign;

/**
 * HTTP client for the internal Campaign Service API.
 * <p>
 * Provides two operations:
 * <ul>
 *   <li>{@link #getCampaignDetails} — fetches campaign details (WaBa, template & quota details, etc)</li>
 *   <li>{@link #getTokenForWhatsappAccount} — fetches the access token for a WaBaPhoneNumberId</li>
 * </ul>
 * <p>
 * Access tokens are always fetched from this service at runtime and never cached.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CampaignClient {

    @Value("${app.internal.campaign-service.url}")
    private String internalApiUrl;

    @Value("${app.internal.campaign-service.access-token}")
    private String accessToken;

    private final HttpClient httpClient;

    private final ObjectMapper objectMapper;

    /**
     * Fetches campaign details (quotas, template IDs) from the Campaign Service.
     */
    public Campaign fetchCampaignDetails(Integer campaignId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(internalApiUrl + "/v1/campaign/" + campaignId))
                .header("Authorization", accessToken)
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return objectMapper.readValue(response.body(), Campaign.class);

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to retrieve campaign details for campaign: {}", campaignId, e);
            return null;
        }
    }

    /**
     * Fetches the access token for a WaBa phone number from the external service API.
     */
    public String fetchTokenForWhatsappAccount(String whatsappBusinessPhoneNumberId) {

        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(internalApiUrl + "/whatsapp-account/" + whatsappBusinessPhoneNumberId))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();

            // For now just response body String as is, assuming I am getting token string directly
            // TODO :: when decided the response body structure update the parsing logic here

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to retrieve access token for account: {}", whatsappBusinessPhoneNumberId, e);
            return null;
        }
    }
}