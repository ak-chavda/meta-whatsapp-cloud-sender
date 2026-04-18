package com.whatsapp.sender.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Campaign(
        String id,
        String name,
        @JsonProperty("whatsapp_business_account_details") List<WhatsAppBusinessAccountDetail> whatsappBusinessAccountDetails,
        @JsonProperty("template_details") List<TemplateDetail> templateDetails) {

    public record WhatsAppBusinessAccountDetail(
            @JsonProperty("wa_ba_id") String waBaId,
            @JsonProperty("wa_ba_phone_number_id") String waBaPhoneNumberId,
            @JsonProperty("daily_sending_limit") int dailySendingLimit) {
    }

    public record TemplateDetail(
            @JsonProperty("template_id") String templateId,
            @JsonProperty("daily_sending_limit") int dailySendingLimit,

            String name,
            Language language,
            List<Component> components) {
    }

    public record Language(String code) {
    }

    /**
     * A single component within a template (header, body, or button).
     *
     * @param type       component type — "header", "body", "button"
     * @param parameters list of parameters for this component
     */
    public record Component(String type, List<Parameter> parameters) {
    }

    /**
     * A single parameter within a {@link Component}.
     * <p>
     * Each parameter maps to a placeholder variable ({{1}}, {{2}}, …).
     * The {@code type} field determines which value field is populated —
     * only one of {@code text} or {@code image} should be non-null.
     *
     * @param type  parameter type — "text", "image", etc.
     * @param text  value when type is "text"
     * @param image media payload when type is "image"
     */
    public record Parameter(String type, String text, Image image) {
    }

    /**
     * Media payload for parameters that reference an external media URL.
     *
     * @param link publicly accessible URL of the media asset
     */
    public record Image(String link) {
    }
}