package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.ws.schemaless.CommonResp;
import com.taosdata.jdbc.ws.stmt.entity.ExecResp;
import com.taosdata.jdbc.ws.stmt.entity.StmtResp;

import java.util.HashMap;
import java.util.Map;

/**
 * request type
 */
public enum Action {
    CONN("conn", ConnectResp.class),
    QUERY("query", QueryResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_JSON("fetch_json", FetchJsonResp.class),
    FETCH_BLOCK("fetch_raw_block", FetchBlockResp.class),
    // free_result's class is meaningless
    FREE_RESULT("free_result", Response.class),

    // stmt
    INIT("init", StmtResp.class),
    PREPARE("prepare", StmtResp.class),
    SET_TABLE_NAME("set_table_name", StmtResp.class),
    SET_TAGS("set_tags", StmtResp.class),
    BIND("bind", StmtResp.class),
    ADD_BATCH("add_batch", StmtResp.class),
    EXEC("exec", ExecResp.class),
    // response means nothing
    CLOSE("close", StmtResp.class),

    //schemaless
    INSERT("insert", CommonResp.class),
    ;
    ;
    private final String action;
    private final Class<? extends Response> clazz;

    Action(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getResponseClazz() {
        return clazz;
    }

    private static final Map<String, Action> actions = new HashMap<>();

    static {
        for (Action value : Action.values()) {
            actions.put(value.action, value);
        }
    }

    public static Action of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}
