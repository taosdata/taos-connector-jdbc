package com.taosdata.jdbc.ws.stmt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.CommonResp;

// init | prepare | set_table_name | set_tags | bind | add_batch
public class StmtResp extends CommonResp {
    @JSONField(name = "stmt_id", deserializeUsing = UInt64Codec.class)
    private long stmtId;
    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }
}
