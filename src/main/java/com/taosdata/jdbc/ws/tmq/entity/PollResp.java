package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.Response;

public class PollResp extends Response {
    private int code;
    private String message;

    @JSONField(name = "have_message")
    private boolean haveMessage;

    private String topic;
    private String database;

    @JSONField(name = "vgroup_id")
    private int vgroupId;

    @JSONField(name = "message_type")
    private int messageType;

    @JSONField(name = "message_id", deserializeUsing = UInt64Codec.class)
    private long messageId;

    private long offset;

    private long timing;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isHaveMessage() {
        return haveMessage;
    }

    public void setHaveMessage(boolean haveMessage) {
        this.haveMessage = haveMessage;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getVgroupId() {
        return vgroupId;
    }

    public void setVgroupId(int vgroupId) {
        this.vgroupId = vgroupId;
    }

    public int getMessageType() {
        return messageType;
    }

    public void setMessageType(int messageType) {
        this.messageType = messageType;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
