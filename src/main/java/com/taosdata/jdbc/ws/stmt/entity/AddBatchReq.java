package com.taosdata.jdbc.ws.stmt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class AddBatchReq extends Payload {
    @JSONField(name = "stmt_id")
    private long stmtId;

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }
}
