package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.enums.TmqMessageType;
import com.taosdata.jdbc.ws.tmq.meta.Meta;

public class ConsumerRecord<V> {
    private final String topic;

    private final String dbName;
    private final int vGroupId;
    private final long offset;

    private final TmqMessageType messageType;

    private final Meta meta;
    private final V value;

    // 私有构造函数，只能通过 Builder 来创建对象
    private ConsumerRecord(Builder<V> builder) {
        this.topic = builder.topic;
        this.dbName = builder.dbName;
        this.vGroupId = builder.vGroupId;
        this.offset = builder.offset;
        this.messageType = builder.messageType;
        this.meta = builder.meta;
        this.value = builder.value;
    }
    public String getTopic() {
        return topic;
    }

    public String getDbName() {
        return dbName;
    }

    public int getVGroupId() {
        return vGroupId;
    }

    public V value() {
        return value;
    }

    public long getOffset() {
        return offset;
    }

    public TmqMessageType getMessageType() {
        return messageType;
    }

    public Meta getMeta() {
        return meta;
    }


    public static class Builder<V> {
        private String topic;
        private String dbName;
        private Integer vGroupId;
        private long offset = 0; // 可以设置默认值
        private TmqMessageType messageType;

        private Meta meta;
        private V value;
        public Builder<V> topic(String topic) {
            this.topic = topic;
            return this;
        }
        public Builder<V> dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }
        public Builder<V> vGroupId(int vGroupId) {
            this.vGroupId = vGroupId;
            return this;
        }
        public Builder<V> offset(long offset) {
            this.offset = offset;
            return this;
        }
        public Builder<V> messageType(TmqMessageType messageType) {
            this.messageType = messageType;
            return this;
        }

        public Builder<V> meta(Meta meta) {
            this.meta = meta;
            return this;
        }
        public Builder<V> value(V value) {
            this.value = value;
            return this;
        }
        public ConsumerRecord<V> build() {
            // 检查必要字段是否已设置
            if (messageType == TmqMessageType.TMQ_RES_DATA){
                if (topic == null || dbName == null || vGroupId == null || value == null) {
                    throw new IllegalStateException("for data type, Topic, dbName, vGroupId and value are required.");
                }
            } else if (messageType == TmqMessageType.TMQ_RES_TABLE_META){
                // 检查必要字段是否已设置
                if (topic == null || dbName == null || vGroupId == null || meta == null) {
                    throw new IllegalStateException("for meta type, Topic, dbName, vGroupId, meta are required.");
                }
            } else {
                throw new IllegalStateException("Unknown messageType: " + messageType);
            }

            return new ConsumerRecord<>(this);
        }
    }
}
