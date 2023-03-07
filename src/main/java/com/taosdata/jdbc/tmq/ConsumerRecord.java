package com.taosdata.jdbc.tmq;

public class ConsumerRecord<V> {
    private final String topic;

    private final String dbName;
    private final int vGroupId;
    private final V value;

    public ConsumerRecord(String topic,
                          String dbName,
                          int vGroupId,
                          V value) {
        this.topic = topic;
        this.dbName = dbName;
        this.vGroupId = vGroupId;
        this.value = value;
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
}
