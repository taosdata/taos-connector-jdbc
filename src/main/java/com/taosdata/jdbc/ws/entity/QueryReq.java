package com.taosdata.jdbc.ws.entity;

public class QueryReq extends Payload {
    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
