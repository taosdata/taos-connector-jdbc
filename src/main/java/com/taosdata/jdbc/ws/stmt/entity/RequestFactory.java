package com.taosdata.jdbc.ws.stmt.entity;


import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Request;

/**
 * generate id for request
 */
public class RequestFactory {

    private RequestFactory() {
    }

    public static Request generateInit(long reqId) {
        InitReq initReq = new InitReq();
        initReq.setReqId(reqId);
        return new Request(Action.INIT.getAction(), initReq);
    }

    public static Request generatePrepare(long stmtId, long reqId, String sql) {
        PrepareReq prepareReq = new PrepareReq();
        prepareReq.setReqId(reqId);
        prepareReq.setStmtId(stmtId);
        prepareReq.setSql(sql);
        return new Request(Action.PREPARE.getAction(), prepareReq);
    }

    public static Request generateSetTableName(long stmtId, long reqId, String tableName) {
        SetTableNameReq req = new SetTableNameReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        req.setName(tableName);
        return new Request(Action.SET_TABLE_NAME.getAction(), req);
    }

    public static Request generateSetTags(long stmtId, long reqId, Object[] tags) {
        SetTagReq req = new SetTagReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        req.setTags(tags);
        return new Request(Action.SET_TAGS.getAction(), req);
    }

    public static Request generateBind(long stmtId, long reqId, Object[][] columns) {
        BindReq req = new BindReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        req.setColumns(columns);
        return new Request(Action.BIND.getAction(), req);
    }

    public static Request generateBatch(long stmtId, long reqId) {
        AddBatchReq req = new AddBatchReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.ADD_BATCH.getAction(), req);
    }

    public static Request generateExec(long stmtId, long reqId) {
        ExecReq req = new ExecReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.EXEC.getAction(), req);
    }

    public static Request generateClose(long stmtId, long reqId) {
        CloseReq req = new CloseReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.EXEC.getAction(), req);
    }

}
