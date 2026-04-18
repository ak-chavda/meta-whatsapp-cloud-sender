package com.whatsapp.sender.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Parses Meta Graph API error responses.
 * Expected format:
 * {
 *   "error": {
 *     "message": "(#130429) Rate limit hit",
 *     "type": "OAuthException",
 *     "code": 130429,
 *     "fbtrace_id": "..."
 *   }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetaApiErrorResponse(ErrorDetails error) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ErrorDetails(int code, String message, String type, String fbtrace_id) {}
}
