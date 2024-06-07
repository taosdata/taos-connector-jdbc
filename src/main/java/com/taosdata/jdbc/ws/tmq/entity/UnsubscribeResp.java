package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.CommonResp;

public class UnsubscribeResp extends CommonResp {
    private long timing;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }
}
