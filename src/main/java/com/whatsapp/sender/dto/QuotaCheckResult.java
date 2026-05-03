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
        String wabaId,
        String wabaPhoneNumberId,
        String templateId,
        String reason,
        ExhaustionType exhaustionType
) {
    public enum ExhaustionType {
        WABA, TEMPLATE, BURST, NONE
    }

    /** Factory for a successful quota check. */
    public static QuotaCheckResult allowed(String wabaId, String wabaPhoneNumberId, String templateId) {
        return new QuotaCheckResult(true, wabaId, wabaPhoneNumberId, templateId, null, ExhaustionType.NONE);
    }

    /** Factory for an exhausted quota (WaBa daily quota — 80007). */
    public static QuotaCheckResult exhaustedWaba(String reason) {
        return new QuotaCheckResult(false, null, null, null, reason, ExhaustionType.WABA);
    }

    /** Factory for an exhausted quota (Template). */
    public static QuotaCheckResult exhaustedTemplate(String reason) {
        return new QuotaCheckResult(false, null, null, null, reason, ExhaustionType.TEMPLATE);
    }

    /** Factory for an exhausted burst/MPS limit (130429 — all phone numbers hit). */
    public static QuotaCheckResult exhaustedBurst(String reason) {
        return new QuotaCheckResult(false, null, null, null, reason, ExhaustionType.BURST);
    }

    /** Factory for when all combinations are exhausted. */
    public static QuotaCheckResult exhausted(String reason) {
        return new QuotaCheckResult(false, null, null, null, reason, ExhaustionType.NONE);
    }
}