package com.whatsapp.sender.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pre-flight kill switch that checks campaign status in Redis before
 * processing a consumed batch.
 * <p>
 * Redis key pattern: {@code campaign:{campaignId}:status}
 * <p>
 * If the campaign status is {@code PAUSED} or {@code CANCELLED}, the batch
 * is immediately discarded. This provides a near-instantaneous mechanism for
 * operators to halt message sending without waiting for in-flight batches
 * to drain.
 * <p>
 * Graceful degradation: If Redis is unreachable, the batch is allowed to
 * proceed (fail-open) to prevent systemic blocking. A failing Redis should
 * trigger alerts via the monitoring layer, not silently drop batches.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KillSwitchService {

    private static final String CAMPAIGN_KILL_SWITCH_STATUS = "whatsapp:campaign:%s:status";
    private static final Set<String> BLOCKED_STATUSES = Set.of("PAUSED", "CANCELLED");

    private final StringRedisTemplate redisTemplate;

    /**
     * Checks whether the campaign is in a blocked state.
     */
    public boolean shouldDiscardBatch(Integer campaignId) {
        try {
            String key = String.format(CAMPAIGN_KILL_SWITCH_STATUS, String.valueOf(campaignId));
            String status = redisTemplate.opsForValue().get(key);

            if (status != null && BLOCKED_STATUSES.contains(status.toUpperCase())) {
                log.warn("Kill switch activated for campaign {} | Status: {}. Discarding batch.", campaignId, status);
                return true;
            }
            return false;

        } catch (Exception e) {
            // Fail-open: if Redis is down, allow batch processing to continue.
            // The monitoring stack should alert on Redis failures separately.
            e.printStackTrace();
            log.error("Kill switch Redis check failed for campaign [{}]. Proceeding with batch processing. Error: {}", campaignId, e.getMessage());
            return false;
        }
    }
}