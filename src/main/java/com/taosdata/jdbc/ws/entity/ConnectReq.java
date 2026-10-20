/*
 * Copyright (c) 2023 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.utils.ProductUtil;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;

/**
 * connection request pojo
 */

public class ConnectReq extends Payload implements Printable {

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
    @JsonProperty("connector")
    private String connector;
    @JsonProperty("bearer_token")
    private String bearerToken;
    @JsonProperty("list_instances")
    private Boolean listInstances;
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
    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getBearerToken() {
        return bearerToken;
    }

    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    public Boolean getListInstances() {
        return listInstances;
    }

    public void setListInstances(Boolean listInstances) {
        this.listInstances = listInstances;
    }

    public ConnectReq(ConnectionParam param) {
        this(param, shouldListInstances(param) ? Boolean.TRUE : null);
    }

    public ConnectReq(ConnectionParam param, Boolean listInstances) {
        this.setReqId(ReqId.getReqID());
        this.setUser(param.getUser());
        this.setPassword(param.getPassword());
        this.setDb(param.getDatabase());
        this.setTz(param.getTz());
        this.setApp(param.getAppName());
        this.setIp(param.getAppIp());
        this.setConnector(ProductUtil.getWsConnectorVersion());
        this.setBearerToken(param.getBearerToken());
        this.setListInstances(listInstances);

        // Currently, only BI mode is supported. The downstream interface value is 0, so a conversion is performed here.
        if(param.getConnectMode() == ConnectionParam.CONNECT_MODE_BI){
            this.setMode(0);
        }
    }

    private static boolean shouldListInstances(ConnectionParam param) {
        return param.isAdapterHa() && StringUtils.isEmpty(param.getSlaveClusterHost());
    }

    @Override
    public String toPrintString() {
        return new StringBuilder("ConnectReq{")
                .append("user='").append(user).append('"')
                .append(", password='").append("******").append('"')
                .append(", bearerToken='").append("******").append('"')
                .append(", db='").append(db).append('"')
                .append(", mode=").append(mode)
                .append(", tz='").append(tz).append('"')
                .append(", app='").append(app).append('"')
                .append(", ip='").append(ip).append('"')
                .append(", connector='").append(connector).append('"')
                .append(", listInstances=").append(listInstances)
                .append(", reqId=").append(getReqId())
                .append('}')
                .toString();
    }
}
