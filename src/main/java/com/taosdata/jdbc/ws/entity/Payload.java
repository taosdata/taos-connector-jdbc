package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.annotation.JSONField;

public class Payload {
    @JSONField(name = "req_id")
    private long reqId;

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public long getReqId() {
        return reqId;
    }
}