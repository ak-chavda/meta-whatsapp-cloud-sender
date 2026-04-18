package com.whatsapp.sender.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Arrays;
import java.util.Objects;

/**
 * Response from the WhatsApp Cloud API for a send-message call.
 * <p>
 * Example successful response:
 * <pre>{@code
 * {
 *   "messaging_product": "whatsapp",
 *   "contacts": [{ "input": "919876543210", "wa_id": "919876543210" }],
 *   "messages": [{ "id": "wamid.xxx" }]
 * }
 * }</pre>
 *
 * @param messagingProduct should always be "whatsapp"
 * @param contacts         matched contacts
 * @param messages         sent message IDs
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WhatsappApiResponse(String messagingProduct, Contact[] contacts, Message[] messages) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Contact(String input, String wa_id) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Message(String id) {}

    /**
     * Extracts the WhatsApp message ID from the first message in the response.
     *
     * @return message ID or {@code null} if unavailable
     */
    public String extractMessageId() {
        if (messages != null && messages.length > 0) {
            return messages[0].id();
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WhatsappApiResponse that)) return false;
        return Objects.equals(messagingProduct, that.messagingProduct)
                && Arrays.equals(contacts, that.contacts)
                && Arrays.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(messagingProduct);
        result = 31 * result + Arrays.hashCode(contacts);
        result = 31 * result + Arrays.hashCode(messages);
        return result;
    }

    @Override
    public String toString() {
        return "WhatsappApiResponse[" +
                "messagingProduct=" + messagingProduct +
                ", contacts=" + Arrays.toString(contacts) +
                ", messages=" + Arrays.toString(messages) +
                ']';
    }
}