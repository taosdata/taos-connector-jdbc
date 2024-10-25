package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.ws.stmt.entity.ExecResp;
import com.taosdata.jdbc.ws.stmt.entity.GetColFieldsResp;
import com.taosdata.jdbc.ws.stmt.entity.StmtResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import com.taosdata.jdbc.ws.tmq.entity.FetchRawBlockResp;

import java.util.HashMap;
import java.util.Map;

/**
 * request type
 */
public enum Action {
    CONN("conn", ConnectResp.class),
    QUERY("query", QueryResp.class),
    BINARY_QUERY("binary_query", QueryResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_BLOCK("fetch_raw_block", FetchRawBlockResp.class),
    FETCH_BLOCK_NEW("fetch_block_new", FetchBlockNewResp.class),

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
    GET_COL_FIELDS("get_col_fields", GetColFieldsResp.class),

    CLOSE("close", StmtResp.class),

    // stmt2
    STMT2_INIT("stmt2_init", Stmt2Resp.class),
    STMT2_PREPARE("stmt2_prepare", Stmt2Resp.class),
    STMT2_BIND("bind", Stmt2Resp.class),
    STMT2_EXEC("exec", ExecResp.class),
    // response means nothing
    STMT2_CLOSE("close", StmtResp.class),

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
