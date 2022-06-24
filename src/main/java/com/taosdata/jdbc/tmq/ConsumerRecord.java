package com.taosdata.jdbc.tmq;

public class ConsumerRecord<V> {
    private String key;
    private V value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
