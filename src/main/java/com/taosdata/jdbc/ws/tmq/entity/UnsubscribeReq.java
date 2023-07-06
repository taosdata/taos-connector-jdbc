package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class UnsubscribeReq extends Payload {
    @JSONField(name = "req_id")
    private long reqId;

    @Override
    public long getReqId() {
        return reqId;
    }

    @Override
    public void setReqId(long reqId) {
        this.reqId = reqId;
    }
}
