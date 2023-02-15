package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.tmq.TMQAction;

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
        for (TMQAction value : TMQAction.values()) {
            ids.put(value.getAction(), new AtomicLong(0));
        }
    }

    public Request generateSubscribe(String user, String password, String db, String groupId,
                                     String clientId, String offsetRest, String[] topics) {
        long reqId = this.getId(TMQAction.SUBSCRIBE.getAction());

        SubscribeReq subscribeReq = new SubscribeReq();
        subscribeReq.setReqId(reqId);
        subscribeReq.setUser(user);
        subscribeReq.setPassword(password);
        subscribeReq.setDb(db);
        subscribeReq.setGroupId(groupId);
        subscribeReq.setClientId(clientId);
        subscribeReq.setOffsetRest(offsetRest);
        subscribeReq.setTopics(topics);
        return new Request(TMQAction.SUBSCRIBE.getAction(), subscribeReq);
    }

    public Request generatePoll(long blockingTime) {
        long reqId = this.getId(TMQAction.POLL.getAction());
        PollReq pollReq = new PollReq();
        pollReq.setReqId(reqId);
        pollReq.setBlockingTime(blockingTime);
        return new Request(TMQAction.POLL.getAction(), pollReq);
    }

    public Request generateFetch(long messageId) {
        long reqId = this.getId(TMQAction.FETCH.getAction());
        FetchReq fetchReq = new FetchReq();
        fetchReq.setReqId(reqId);
        fetchReq.setMessageId(messageId);
        return new Request(TMQAction.FETCH.getAction(), fetchReq);
    }

    public Request generateFetchBlock(long fetchRequestId,long messageId) {
        FetchBlockReq fetchBlockReq = new FetchBlockReq();
        fetchBlockReq.setReqId(fetchRequestId);
        fetchBlockReq.setMessageId(messageId);
        return new Request(TMQAction.FETCH_BLOCK.getAction(), fetchBlockReq);
    }

    public Request generateCommit(long messageId) {
        long reqId = this.getId(TMQAction.COMMIT.getAction());
        CommitReq commitReq = new CommitReq();
        commitReq.setReqId(reqId);
        commitReq.setMessageId(messageId);
        return new Request(TMQAction.COMMIT.getAction(), commitReq);
    }
}
