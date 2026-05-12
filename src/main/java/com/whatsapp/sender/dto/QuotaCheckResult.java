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
 * @param allowed                   whether the current WaBa + template has available quota
 * @param preserveWaBaPhoneNumberId true if the WaBa phone number ID should be preserved for future use, false otherwise
 * @param wabaPhoneNumberId         the resolved WaBa phone number ID to use
 * @param preserveTemplateId        true if the Template ID should be preserved for future use, false otherwise
 * @param templateId                the resolved Template ID to use
 * @param exhaustionType            the type of exhaustion (WABA, TEMPLATE, BURST, or NONE)
 * @param reason                    human-readable reason if not allowed (null if allowed)
 */
public record QuotaCheckResult(

        boolean allowed,
        boolean preserveWaBaPhoneNumberId,
        String wabaPhoneNumberId,
        boolean preserveTemplateId,
        String templateId,
        ExhaustionType exhaustionType,
        String reason) {

    public enum ExhaustionType {
        WABA_DAILY_QUOTA_EXHAUSTED, TEMPLATE_DAILY_QUOTA_EXHAUSTED, WABA_PHONENUMBER_BURST_QUOTA_EXHAUSTED, NONE
    }

    public static QuotaCheckResult allowed(String wabaPhoneNumberId, String templateId) {
        return new QuotaCheckResult(true, false, wabaPhoneNumberId, false, templateId, ExhaustionType.NONE, null);
    }

    public static QuotaCheckResult exhausted(ExhaustionType type, String reason) {
        return new QuotaCheckResult(false, false, null, false, null, type, reason);
    }

    public static QuotaCheckResult allowedWithPreserve(boolean preserveWaBaPhoneNumberId, String wabaPhoneNumberId, boolean preserveTemplateId, String templateId) {
        return new QuotaCheckResult(true, preserveWaBaPhoneNumberId, wabaPhoneNumberId, preserveTemplateId, templateId, ExhaustionType.NONE, null);
    }

    public static QuotaCheckResult exhaustedWithPreserve(boolean preserveWaBaPhoneNumberId, String wabaPhoneNumberId, boolean preserveTemplateId, String templateId, ExhaustionType type, String reason) {
        return new QuotaCheckResult(false, preserveWaBaPhoneNumberId, wabaPhoneNumberId, preserveTemplateId, templateId, type, reason);
    }
}