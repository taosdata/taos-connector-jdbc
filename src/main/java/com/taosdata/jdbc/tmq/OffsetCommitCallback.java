package com.taosdata.jdbc.tmq;

@FunctionalInterface
public interface OffsetCommitCallback<V> {

    void onComplete(ConsumerRecords<V> records, Exception exception);
}
