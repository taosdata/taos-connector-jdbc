package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class CommitOffsetReq extends Payload {
    private String topic;

    @JSONField(name = "vgroup_id")
    private int vgroupId;

    private long offset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getVgroupId() {
        return vgroupId;
    }

    public void setVgroupId(int vgId) {
        this.vgroupId = vgId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
