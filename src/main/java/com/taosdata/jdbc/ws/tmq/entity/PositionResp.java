package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.ws.entity.CommonResp;

public class PositionResp extends CommonResp {
    private long timing;
    private long[] position;

    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public long[] getPosition() {
        return position;
    }

    public void setPosition(long[] position) {
        this.position = position;
    }
}
