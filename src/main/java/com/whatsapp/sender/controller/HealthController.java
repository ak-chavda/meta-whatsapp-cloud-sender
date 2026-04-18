package com.whatsapp.sender.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * Lightweight health endpoint for readiness probes and service discovery.
 * <p>
 * Supplements Spring Actuator with application-specific metadata.
 * Kubernetes/ECS readiness probes should target {@code /api/health}.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    @Value("${spring.application.name}")
    private String applicationName;

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "service", applicationName,
                "status", "UP",
                "timestamp", Instant.now().toString(),
                "thread", Thread.currentThread().toString()
        );
    }
}
