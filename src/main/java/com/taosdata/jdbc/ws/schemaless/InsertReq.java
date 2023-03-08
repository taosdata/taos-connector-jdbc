package com.taosdata.jdbc.ws.schemaless;

import com.taosdata.jdbc.ws.entity.Payload;

public class InsertReq extends Payload {
    // database
    private String db;
    private int protocol;
    private int precision;
    private String data;
    private int ttl;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
