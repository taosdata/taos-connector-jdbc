package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.Payload;

public class AssignmentReq extends Payload {
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
