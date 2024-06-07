package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class CommitResp extends CommonResp {
    @JSONField(name = "message_id", deserializeUsing = UInt64Codec.class)
    private long messageId;

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }
}
