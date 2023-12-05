package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.tmq.TopicPartition;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.tmq.ConsumerAction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * generate id for request
 */
public class TMQRequestFactory {
    private final Map<String, AtomicLong> ids = new HashMap<>();

    public long getId(String action) {
        return ids.get(action).incrementAndGet();
    }

    public TMQRequestFactory() {
        for (ConsumerAction value : ConsumerAction.values()) {
            ids.put(value.getAction(), new AtomicLong(0));
        }
    }

    public Request generateSubscribe(String user, String password, String db, String groupId,
                                     String clientId, String offsetRest, String[] topics
            , String enableAutoCommit, String autoCommitIntervalMs, String withTableName) {
        long reqId = this.getId(ConsumerAction.SUBSCRIBE.getAction());

        SubscribeReq subscribeReq = new SubscribeReq();
        subscribeReq.setReqId(reqId);
        subscribeReq.setUser(user);
        subscribeReq.setPassword(password);
        subscribeReq.setDb(db);
        subscribeReq.setGroupId(groupId);
        subscribeReq.setClientId(clientId);
        subscribeReq.setOffsetRest(offsetRest);
        subscribeReq.setTopics(topics);
        subscribeReq.setAutoCommit(enableAutoCommit);
        subscribeReq.setAutoCommitIntervalMs(autoCommitIntervalMs);
        subscribeReq.setWithTableName(withTableName);
        return new Request(ConsumerAction.SUBSCRIBE.getAction(), subscribeReq);
    }

    public Request generatePoll(long blockingTime) {
        long reqId = this.getId(ConsumerAction.POLL.getAction());
        PollReq pollReq = new PollReq();
        pollReq.setReqId(reqId);
        pollReq.setBlockingTime(blockingTime);
        return new Request(ConsumerAction.POLL.getAction(), pollReq);
    }

    public Request generateFetch(long messageId) {
        long reqId = this.getId(ConsumerAction.FETCH.getAction());
        FetchReq fetchReq = new FetchReq();
        fetchReq.setReqId(reqId);
        fetchReq.setMessageId(messageId);
        return new Request(ConsumerAction.FETCH.getAction(), fetchReq);
    }

    public Request generateFetchBlock(long fetchRequestId, long messageId) {
        FetchBlockReq fetchBlockReq = new FetchBlockReq();
        fetchBlockReq.setReqId(fetchRequestId);
        fetchBlockReq.setMessageId(messageId);
        return new Request(ConsumerAction.FETCH_BLOCK.getAction(), fetchBlockReq);
    }

    public Request generateCommit(long messageId) {
        long reqId = this.getId(ConsumerAction.COMMIT.getAction());
        CommitReq commitReq = new CommitReq();
        commitReq.setReqId(reqId);
        commitReq.setMessageId(messageId);
        return new Request(ConsumerAction.COMMIT.getAction(), commitReq);
    }

    public Request generateSeek(String topic, int vgId, long offset) {
        long reqId = this.getId(ConsumerAction.SEEK.getAction());
        SeekReq seekReq = new SeekReq();
        seekReq.setReqId(reqId);
        seekReq.setTopic(topic);
        seekReq.setVgId(vgId);
        seekReq.setOffset(offset);
        return new Request(ConsumerAction.SEEK.getAction(), seekReq);
    }

    public Request generateAssignment(String topic) {
        long reqId = this.getId(ConsumerAction.ASSIGNMENT.getAction());
        AssignmentReq assignmentReq = new AssignmentReq();
        assignmentReq.setReqId(reqId);
        assignmentReq.setTopic(topic);
        return new Request(ConsumerAction.ASSIGNMENT.getAction(), assignmentReq);
    }

    public Request generateUnsubscribe() {
        long reqId = this.getId(ConsumerAction.UNSUBSCRIBE.getAction());
        UnsubscribeReq unsubscribeReq = new UnsubscribeReq();
        unsubscribeReq.setReqId(reqId);
        return new Request(ConsumerAction.UNSUBSCRIBE.getAction(), unsubscribeReq);
    }

    public Request generateCommitted(TopicPartition[] topicPartitions) {
        long reqId = this.getId(ConsumerAction.COMMITTED.getAction());
        CommittedReq committedReq = new CommittedReq();
        committedReq.setReqId(reqId);
        committedReq.setTopicPartitions(topicPartitions);
        return new Request(ConsumerAction.COMMITTED.getAction(), committedReq);
    }

    public Request generatePosition(TopicPartition[] topicPartitions){
        long reqId = this.getId(ConsumerAction.POSITION.getAction());
        PositionReq positionReq = new PositionReq();
        positionReq.setReqId(reqId);
        positionReq.setTopicPartitions(topicPartitions);
        return new Request(ConsumerAction.POSITION.getAction(), positionReq);
    }

    public Request generateSubscription(){
        long reqId = this.getId(ConsumerAction.LIST_TOPICS.getAction());
        ListTopicsReq listTopicsReq = new ListTopicsReq();
        listTopicsReq.setReqId(reqId);
        return new Request(ConsumerAction.LIST_TOPICS.getAction(), listTopicsReq);
    }

    public Request generateCommitOffset(TopicPartition topicPartition, long offset){
        long reqId = this.getId(ConsumerAction.COMMIT_OFFSET.getAction());
        CommitOffsetReq commitOffsetReq = new CommitOffsetReq();
        commitOffsetReq.setReqId(reqId);
        commitOffsetReq.setTopic(topicPartition.getTopic());
        commitOffsetReq.setVgroupId(topicPartition.getVGroupId());
        commitOffsetReq.setOffset(offset);
        return new Request(ConsumerAction.COMMIT_OFFSET.getAction(), commitOffsetReq);
    }
}
