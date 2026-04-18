package com.whatsapp.sender.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.whatsapp.sender.dto.Campaign.Component;
import com.whatsapp.sender.dto.Campaign.Language;

/**
 * Maps directly to the JSON payload for
 * {@code POST /{WHATSAPP_BUSINESS_PHONE_NUMBER_ID}/messages}.
 * Static fields ({@code messaging_product}, {@code recipient_type},
 * {@code type})
 * are pre-set by the convenience constructor.
 * 
 * <pre>
 * {
 *   "messaging_product": "whatsapp",
 *   "recipient_type": "individual",
 *   "to": "919876543210",
 *   "type": "template",
 *   "template": {
 *     "name": "summer_sale_2026",
 *     "language": { "code": "en_US" },
 *     "components": [
 *       { "type": "header", "parameters": [{ "type": "image", "image": { "link": "..." } }] },
 *       { "type": "body",   "parameters": [{ "type": "text", "text": "Anish" }] }
 *     ]
 *   }
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WhatsAppTemplateRequest(

        @JsonProperty("messaging_product") String messagingProduct,
        @JsonProperty("recipient_type") String recipientType,

        String to,
        String type,
        Template template) {

    /**
     * Convenience constructor that pre-fills the static Meta API fields.
     *
     * @param to       recipient phone number in E.164 format
     * @param template template details with dynamic components
     */
    public WhatsAppTemplateRequest(String to, Template template) {
        this("whatsapp", "individual", to, "template", template);
    }

    /**
     * Template block — name, language, and dynamic components.
     *
     * @param name       template name registered in Meta Business Manager
     * @param language   language selection for the template
     * @param components list of header / body / button components with dynamic
     *                   parameters
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Template(
            String name,
            Language language,
            List<Component> components) {
    }
}