package com.whatsapp.sender.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.whatsapp.sender.dto.Campaign;

/**
 * Redis-backed caching layer for Campaign Details.
 * <p>
 * Wraps the upstream {@link campaignClient} with a Redis cache to avoid
 * making network calls for every batch. Campaign details (quotas/limits) are
 * cached with a configurable TTL (default 10 minutes).
 * <p>
 * Cache key pattern: {@code campaign:{campaignId}:detail} — CampaignDetail JSON
 * <p>
 * Access tokens are NOT cached — they are always fetched directly from the
 * external service API via {@link campaignClient#getTokenForWhatsappAccount}
 * to avoid stale or leaked tokens in Redis.
 * <p>
 * Graceful degradation: If Redis is unreachable, falls back to the upstream
 * HTTP client directly (fail-open strategy, same as KillSwitchService).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CampaignService {

    private static final String CAMPAIGN_DETAIL_KEY = "campaign:%d:detail";

    private final CampaignClient campaignClient;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.quota.cache-ttl-minutes:10}")
    private int cacheTtlMinutes;

    /**
     * Fetches campaign details, checking Redis cache first.
     * <p>
     * On cache miss, calls the upstream Campaign Service, caches the result,
     * and returns it. On cache hit, deserializes from Redis directly.
     *
     * @param campaignId the campaign to fetch details for
     * @return campaign details, or {@code null} if retrieval fails
     */
    public Campaign getCampaignDetail(Integer campaignId) {
        String cacheKey = String.format(CAMPAIGN_DETAIL_KEY, campaignId);

        try {
            String cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                log.debug("Cache HIT for campaign [{}]", campaignId);
                return objectMapper.readValue(cached, Campaign.class);
            }
        } catch (Exception e) {
            log.warn("Redis cache read failed for campaign [{}]. Falling back to upstream. Error: {}", campaignId, e.getMessage());
        }

        // Cache miss or Redis error — fetch from upstream
        log.debug("Cache MISS for campaign [{}]. Fetching from upstream.", campaignId);
        Campaign campaign = campaignClient.fetchCampaignDetails(campaignId);

        if (campaign != null) {
            try {
                String json = objectMapper.writeValueAsString(campaign);
                redisTemplate.opsForValue().set(cacheKey, json, Duration.ofMinutes(cacheTtlMinutes));
                log.debug("Cached campaign [{}] with TTL {} minutes", campaignId, cacheTtlMinutes);

            } catch (JsonProcessingException e) {
                log.warn("Failed to serialize campaign [{}] for caching: {}", campaignId, e.getMessage());
            } catch (Exception e) {
                log.warn("Redis cache write failed for campaign [{}]: {}", campaignId, e.getMessage());
            }
        }

        return campaign;
    }
}