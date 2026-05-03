package com.whatsapp.sender.dto;

/**
 * Result of a quota pre-check for a specific WaBa number and template.
 * <p>
 * Used by {@code QuotaManager} to determine whether a target can be
 * dispatched to a given WaBa number, or whether rotation to a fallback is needed.
 * <p>
 * Access token is NOT included here — it is fetched separately from the
 * external service API at runtime.
 *
 * @param allowed           whether the current WaBa + template has available quota
 * @param wabaId            the WABA Account ID (for 80007 daily quota scoping)
 * @param wabaPhoneNumberId the resolved WaBa phone number ID to use
 * @param templateId        the resolved Template ID to use
 * @param reason            human-readable reason if not allowed (null if allowed)
 * @param exhaustionType    the type of exhaustion (WABA, TEMPLATE, BURST, or NONE)
 */
public record QuotaCheckResult(
        boolean allowed,
        String wabaPhoneNumberId,
        String templateId,
        ExhaustionType exhaustionType,
        String reason
) {
    public enum ExhaustionType {
        WABA, TEMPLATE, BURST, NONE
    }

    /** Factory for a successful quota check. */
    public static QuotaCheckResult allowed(String wabaPhoneNumberId, String templateId) {
        return new QuotaCheckResult(true, wabaPhoneNumberId, templateId, ExhaustionType.NONE, null);
    }

    public static QuotaCheckResult exhausted(ExhaustionType type, String reason) {
        return new QuotaCheckResult(false, null, null, type, reason);
    }
}