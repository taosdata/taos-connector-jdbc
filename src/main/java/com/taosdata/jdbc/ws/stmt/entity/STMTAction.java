package com.taosdata.jdbc.ws.stmt.entity;


import com.taosdata.jdbc.ws.entity.ConnectResp;
import com.taosdata.jdbc.ws.entity.Response;

import java.util.HashMap;
import java.util.Map;

public enum STMTAction {
    CONN("conn", ConnectResp.class),
    INIT("init", StmtResp.class),
    PREPARE("prepare", StmtResp.class),
    SET_TABLE_NAME("set_table_name", StmtResp.class),
    SET_TAGS("set_tags", StmtResp.class),
    BIND("bind", StmtResp.class),
    ADD_BATCH("add_batch", StmtResp.class),
    EXEC("exec", ExecResp.class),
    // response means nothing
    CLOSE("close", StmtResp.class),
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


    private static final Map<String, STMTAction> actions = new HashMap<>();

    static {
        for (STMTAction value : STMTAction.values()) {
            actions.put(value.action, value);
        }
    }

    public static STMTAction of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}
