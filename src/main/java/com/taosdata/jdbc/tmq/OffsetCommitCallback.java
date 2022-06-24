package com.taosdata.jdbc.tmq;

@FunctionalInterface
public interface OffsetCommitCallback {

    void onComplete(CallbackResult result, Exception exception);
}
