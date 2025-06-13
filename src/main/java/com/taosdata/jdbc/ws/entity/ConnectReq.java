package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;

/**
 * connection request pojo
 */

public class ConnectReq extends Payload {

    @JsonProperty("user")
    private String user;
    @JsonProperty("password")
    private String password;
    @JsonProperty("db")
    private String db;
    @JsonProperty("mode")
    private Integer mode;
    @JsonProperty("tz")
    private String tz;
    @JsonProperty("app")
    private String app;
    @JsonProperty("ip")
    private String ip;

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


    public String getTz() {
        return tz;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public ConnectReq(ConnectionParam param) {
        this.setReqId(ReqId.getReqID());
        this.setUser(param.getUser());
        this.setPassword(param.getPassword());
        this.setDb(param.getDatabase());
        this.setTz(param.getTz());
        this.setApp(param.getAppName());
        this.setIp(param.getAppIp());

        // Currently, only BI mode is supported. The downstream interface value is 0, so a conversion is performed here.
        if(param.getConnectMode() == ConnectionParam.CONNECT_MODE_BI){
            this.setMode(0);
        }
    }
}
