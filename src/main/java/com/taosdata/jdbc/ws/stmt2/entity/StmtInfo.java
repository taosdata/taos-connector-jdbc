package com.taosdata.jdbc.ws.stmt2.entity;

import java.util.List;

public class StmtInfo {
    private long reqId;
    private long stmtId = 0;
    private int toBeBindTableNameIndex;
    private int toBeBindTagCount;
    private int toBeBindColCount;
    private int precision;
    private final List<Field> fields;
    private final String sql;
    public StmtInfo(long reqId,
                    long stmtId,
                    int toBeBindTableNameIndex,
                    int toBeBindTagCount,
                    int toBeBindColCount,
                    int precision,
                    List<Field> fields,
                    String sql) {
        this.reqId = reqId;
        this.stmtId = stmtId;
        this.toBeBindTableNameIndex = toBeBindTableNameIndex;
        this.toBeBindTagCount = toBeBindTagCount;
        this.toBeBindColCount = toBeBindColCount;
        this.precision = precision;
        this.fields = fields;
        this.sql = sql;
    }

    public long getReqId() {
        return reqId;
    }

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public int getToBeBindTableNameIndex() {
        return toBeBindTableNameIndex;
    }

    public void setToBeBindTableNameIndex(int toBeBindTableNameIndex) {
        this.toBeBindTableNameIndex = toBeBindTableNameIndex;
    }

    public int getToBeBindTagCount() {
        return toBeBindTagCount;
    }

    public void setToBeBindTagCount(int toBeBindTagCount) {
        this.toBeBindTagCount = toBeBindTagCount;
    }

    public int getToBeBindColCount() {
        return toBeBindColCount;
    }

    public void setToBeBindColCount(int toBeBindColCount) {
        this.toBeBindColCount = toBeBindColCount;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
    public List<Field> getFields() {
        return fields;
    }

    public String getSql() {
        return sql;
    }
}
