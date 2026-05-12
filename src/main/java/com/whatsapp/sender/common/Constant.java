package com.whatsapp.sender.common;

public class Constant {

    private Constant() {
    }

    public static final String CAMPAIGN_DETAIL_KEY = "wa-campaign:%d:detail";

    public static final String CAMPAIGN_KILL_SWITCH_STATUS = "wa-campaign:%s:status";

    public static final String TEMPLATE_QUOTA_USED_COUNTER_KEY = "wa-campaign:waba:%s:template:%s:quota:used";

    public static final String CAMPAIGN_WABA_SUCCESS_COUNT_KEY = "wa-campaign:%s:waba:%s:success_count";

    /** Per Phone Number ID — burst/MPS limit (130429). */
    public static final String WABA_PHONE_NUMBER_RATE_LIMIT_PREFIX = "wa-campaign:circuit:waba:rate-limit:";

    /**
     * Per WABA ID — daily quota limit (80007). All phone numbers share this pool.
     */
    public static final String WABA_DAILY_QUOTA_EXHAUSTED_PREFIX = "wa-campaign:circuit:waba:daily-quota:";

    /** Per Campaign — any template exhausted. */
    public static final String CAMPAIGN_TEMPLATE_EXHAUSTED_PREFIX = "wa-campaign:circuit:waba:%s:template:%s";
}