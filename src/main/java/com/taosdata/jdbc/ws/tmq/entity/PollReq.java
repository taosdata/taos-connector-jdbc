package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class PollReq extends Payload {
    @JsonProperty("blocking_time")
    private long blockingTime;

    public long getBlockingTime() {
        return blockingTime;
    }

    public void setBlockingTime(long blockingTime) {
        this.blockingTime = blockingTime;
    }
}
