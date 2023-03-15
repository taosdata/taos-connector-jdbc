package com.taosdata.jdbc.ws.stmt.entity;

import com.taosdata.jdbc.ws.entity.*;

public enum STMTAction {
    CONN("conn", ConnectResp.class),
    INIT("init", ConnectResp.class),
    PREPARE("prepare", QueryResp.class),
    SET_TABLE_NAME("set_table_name", QueryResp.class),
    SET_TAGS("set_tags", QueryResp.class),
    BIND("bind", QueryResp.class),
    ADD_BATCH("add_batch", QueryResp.class),
    EXEC("exec", QueryResp.class),
    CLOSE("close", QueryResp.class),
    ;
    private final String action;
    private final Class<? extends Response> clazz;

    STMTAction(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getClazz() {
        return clazz;
    }
}
