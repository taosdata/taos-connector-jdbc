package com.taosdata.jdbc.ws.tmq.meta;

public class Sql {
    private String type; // must be delete
    private String sql;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
