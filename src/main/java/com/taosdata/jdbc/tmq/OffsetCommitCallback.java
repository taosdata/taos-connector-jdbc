package com.taosdata.jdbc.tmq;

@FunctionalInterface
public interface OffsetCommitCallback {

    void onComplete(ConsumerRecords records, Exception exception);
}
