package com.taosdata.jdbc.tmq;

public class ConsumerRecord<V> {
    private V value;

    public ConsumerRecord(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
