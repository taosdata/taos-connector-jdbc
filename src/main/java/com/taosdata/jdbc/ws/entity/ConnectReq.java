package com.taosdata.jdbc.ws.entity;

/**
 * connection request pojo
 */

public class ConnectReq extends Payload {

    private String user;
    private String password;
    private String db;
    private Integer mode;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }
}
