package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.tmq.entity.*;

import java.util.HashMap;
import java.util.Map;

public enum ConsumerAction {
    // subscribe
    SUBSCRIBE("subscribe", SubscribeResp.class),
    POLL("poll", PollResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_BLOCK("fetch_block", FetchBlockResp.class),
    COMMIT("commit", CommitResp.class),
    ;

    private final String action;
    private final Class<? extends Response> clazz;

    ConsumerAction(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getResponseClazz() {
        return clazz;
    }

    private static final Map<String, ConsumerAction> actions = new HashMap<>();

    static {
        for (ConsumerAction value : ConsumerAction.values()) {
            actions.put(value.action, value);
        }
    }

    public static ConsumerAction of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}
