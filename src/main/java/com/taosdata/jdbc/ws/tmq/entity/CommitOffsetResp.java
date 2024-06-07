package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class CommitOffsetResp extends CommonResp {
    private long timing;
    private String topic;
    @JSONField(name = "vg_id")
    private int vgId;
    private long offset;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getVgId() {
        return vgId;
    }

    public void setVgId(int vgId) {
        this.vgId = vgId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
