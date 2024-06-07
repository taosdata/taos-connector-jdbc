package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.CommonResp;

public class ListTopicsResp extends CommonResp {
    private long timing;
    private String[] topics;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }
}
