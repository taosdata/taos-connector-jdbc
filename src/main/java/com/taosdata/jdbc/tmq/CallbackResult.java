package com.taosdata.jdbc.tmq;


import java.util.List;

public class CallbackResult<V> {
    private final int code;
    private final List<ConsumerRecord<V>> recordList;

    public CallbackResult(int code, List<ConsumerRecord<V>> recordList) {
        this.code = code;
        this.recordList = recordList;
    }

    public int getCode() {
        return code;
    }

    public List<ConsumerRecord<V>> getRecordList() {
        return recordList;
    }
}
