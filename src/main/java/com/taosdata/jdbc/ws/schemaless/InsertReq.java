package com.taosdata.jdbc.ws.schemaless;

import com.taosdata.jdbc.ws.entity.Payload;

public class InsertReq extends Payload {

    private int protocol;
    private String precision;
    private String data;
    private int ttl;

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }

    public String getPrecision() {
        return precision;
    }

    public void setPrecision(String precision) {
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
