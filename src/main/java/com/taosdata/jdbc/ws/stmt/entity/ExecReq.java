package com.taosdata.jdbc.ws.stmt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.Payload;

public class ExecReq extends Payload {
    @JSONField(name = "stmt_id", serializeUsing = UInt64Codec.class)
    private long stmtId;

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }
}
