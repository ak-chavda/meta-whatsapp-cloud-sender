package com.whatsapp.sender.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Campaign(
        Integer id,
        String name,
        @JsonProperty("waba_id") String wabaId,
        @JsonProperty("waba_numbers") List<WabaNumberDetail> wabaNumbers,
        @JsonProperty("templates") List<TemplateDetail> templates) {

    public record WabaNumberDetail(
            @JsonProperty("waba_phone_number_id") String wabaPhoneNumberId) {
    }

    public record TemplateDetail(
            @JsonProperty("template_id") String templateId,
            @JsonProperty("quota") int quota,
            @JsonProperty("quota_reset_in_seconds") long quotaResetInSeconds,

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