package com.taosdata.jdbc.ws.entity;

public class FetchReq extends Payload {
    private long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
