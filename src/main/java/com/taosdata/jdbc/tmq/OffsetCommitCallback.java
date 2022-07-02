package com.taosdata.jdbc.tmq;

@FunctionalInterface
public interface OffsetCommitCallback {

    <V> void onComplete(CallbackResult<V> result, Exception exception);
}
