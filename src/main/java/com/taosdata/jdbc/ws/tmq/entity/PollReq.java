package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class PollReq extends Payload {
    @JSONField(name = "blocking_time")
    private long blockingTime;

    public long getBlockingTime() {
        return blockingTime;
    }

    public void setBlockingTime(long blockingTime) {
        this.blockingTime = blockingTime;
    }
}
