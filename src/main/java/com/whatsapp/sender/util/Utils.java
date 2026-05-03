package com.whatsapp.sender.util;

import com.whatsapp.sender.dto.Campaign;
import com.whatsapp.sender.dto.Campaign.TemplateDetail;
import com.whatsapp.sender.dto.Campaign.WabaNumberDetail;

public class Utils {

    private Utils() {
    }

    public static TemplateDetail findTemplateDetail(Campaign campaign, String templateId) {
        if (campaign.templates() == null)
            return null;
        return campaign.templates().stream()
                .filter(t -> templateId.equals(t.templateId()))
                .findFirst()
                .orElse(null);
    }

    public static WabaNumberDetail findWabaDetail(Campaign campaign, String wabaPhoneNumberId) {
        if (campaign.wabaNumbers() == null)
            return null;
        return campaign.wabaNumbers().stream()
                .filter(w -> wabaPhoneNumberId.equals(w.wabaPhoneNumberId()))
                .findFirst()
                .orElse(null);
    }
}