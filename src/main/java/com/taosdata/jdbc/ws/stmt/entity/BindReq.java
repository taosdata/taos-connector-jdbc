package com.taosdata.jdbc.ws.stmt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.Payload;

public class BindReq extends Payload {
    @JSONField(name = "stmt_id", serializeUsing = UInt64Codec.class)
    private long stmtId;
    private Object[] columns;

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public Object[] getColumns() {
        return columns;
    }

    public void setColumns(Object[] columns) {
        this.columns = columns;
    }
}
