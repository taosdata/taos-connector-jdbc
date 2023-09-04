package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;

public class FetchReq extends Payload {
    @JSONField(serializeUsing = UInt64Codec.class)
    private long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
