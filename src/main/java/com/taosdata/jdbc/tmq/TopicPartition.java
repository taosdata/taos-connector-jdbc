package com.taosdata.jdbc.tmq;

import java.util.Objects;

public class TopicPartition {
    private final String topic;
    private final String databaseName;
    private final int vgroupId;

    public TopicPartition(String topic, String databaseName, int vgroupId) {
        this.topic = topic;
        this.databaseName = databaseName;
        this.vgroupId = vgroupId;
    }

    public String getTopic() {
        return topic;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public int getVgroupId() {
        return vgroupId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition partition = (TopicPartition) o;
        return vgroupId == partition.vgroupId && Objects.equals(topic, partition.topic)
                && Objects.equals(databaseName, partition.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, databaseName, vgroupId);
    }
}
