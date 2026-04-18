package com.whatsapp.sender.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis configuration for the Sender Service.
 * <p>
 * Uses {@link StringRedisTemplate} since all Redis interactions in this service
 * are simple key-value string operations:
 * <ul>
 *   <li>Kill switch check: {@code campaign:{campaignId}:status}</li>
 *   <li>Circuit breaker: {@code circuit:waba:rate-limit:{phoneNumberId}}</li>
 *   <li>Quota counters: {@code quota:waba:{id}:daily:{date}}</li>
 *   <li>Campaign cache: {@code campaign:{campaignId}:detail}</li>
 * </ul>
 * <p>
 * Also registers Redis Lua scripts for atomic quota operations.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

}