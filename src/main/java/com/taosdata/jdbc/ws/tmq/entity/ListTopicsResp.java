package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.Response;

public class ListTopicsResp extends Response {
    private int code;
    private String message;
    private long timing;
    private String[] topics;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

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
