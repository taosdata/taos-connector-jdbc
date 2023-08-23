package com.taosdata.jdbc.tmq;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Objects;

public class TopicPartition {
    private final String topic;
    @JSONField(name = "vgroup_id")
    private final int vGroupId;

    public TopicPartition(String topic, int vGroupId) {
        this.topic = topic;
        this.vGroupId = vGroupId;
    }

    public String getTopic() {
        return topic;
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
        return "TopicPartition{" + "topic='" + topic + '\'' +
                ", vGroupId=" + vGroupId +
                '}';
    }
}
