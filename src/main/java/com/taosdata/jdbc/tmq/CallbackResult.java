package com.taosdata.jdbc.tmq;

import java.util.List;

public class CallbackResult {
    private final int code;
    private final List<?> recordList;

    public CallbackResult(int code, List<?> recordList) {
        this.code = code;
        this.recordList = recordList;
    }

    public int getCode() {
        return code;
    }

    public List<?> getRecordList() {
        return recordList;
    }
}
