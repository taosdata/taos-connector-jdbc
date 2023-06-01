package com.taosdata.jdbc.tmq;

import java.util.Objects;

public class TopicPartition {
    private final String topic;
    private String databaseName;
    private final int vGroupId;

    public TopicPartition(String topic, int vGroupId) {
        this.topic = topic;
        this.vGroupId = vGroupId;
    }

    public TopicPartition(String topic, String databaseName, int vGroupId) {
        this.topic = topic;
        this.databaseName = databaseName;
        this.vGroupId = vGroupId;
    }

    public String getTopic() {
        return topic;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public int getVGroupId() {
        return vGroupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition partition = (TopicPartition) o;
        return vGroupId == partition.vGroupId && Objects.equals(topic, partition.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, vGroupId);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TopicPartition{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", databaseName='").append(databaseName).append('\'');
        sb.append(", vGroupId=").append(vGroupId);
        sb.append('}');
        return sb.toString();
    }
}
