package com.whatsapp.sender.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import com.whatsapp.sender.dao.MessageDispatchDocument;

@Repository
public interface MessageDispatchRepository extends MongoRepository<MessageDispatchDocument, String> {

    public Optional<MessageDispatchDocument> findByCampaignIdAndMobile(Integer campaignId, String mobile);

    public List<MessageDispatchDocument> findByCampaignIdAndMobileIn(Integer campaignId, List<String> mobile);

    /**
     * write a query to fetch the records by campiagn-id & list of mobiles, which attempts are beyond the provided attempts limit (Retry exhausted)
     * db.getCollection("campaign_message_dispatch_log").find({ campaign_id: 292, mobile: { $in: [ "919607009549" ] }, attempts: { $gt: 4 } })
     */
    public List<MessageDispatchDocument> findByCampaignIdAndMobileInAndAttemptsGreaterThan(Integer campaignId, List<String> mobileNumbers, int maxAttemptsLimit);

    /**
     * write a query to fetch the records by campiagn-id & list of mobiles, which attempts are less then the provided attempts limit (Retry NOT exhausted)
     * db.getCollection("campaign_message_dispatch_log").find({ campaign_id: 292, mobile: { $in: [ "919607009549" ] }, attempts: { $lte: 4 } })
     */
    public List<MessageDispatchDocument> findByCampaignIdAndMobileInAndAttemptsLessThanEqual(Integer campaignId, List<String> mobileNumbers, int maxAttemptsLimit);
}