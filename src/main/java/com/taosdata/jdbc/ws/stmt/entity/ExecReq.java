package com.taosdata.jdbc.ws.stmt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class ExecReq extends Payload {
    @JSONField(name = "stmt_id")
    private long stmtId;
}
