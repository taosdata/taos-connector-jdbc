package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class SubscribeReq extends Payload {
    private String user;
    private String password;
    private String db;
    @JsonProperty("group_id")
    private String groupId;
    @JsonProperty("client_id")
    private String clientId;
    @JsonProperty("offset_rest")
    private String offsetRest;
    private String[] topics;

    @JsonProperty("auto_commit")
    private String autoCommit;
    @JsonProperty("auto_commit_interval_ms")
    private String autoCommitIntervalMs;
    @JsonProperty("with_table_name")
    private String withTableName;

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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getOffsetRest() {
        return offsetRest;
    }

    public void setOffsetRest(String offsetRest) {
        this.offsetRest = offsetRest;
    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(String autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public String getWithTableName() {
        return withTableName;
    }

    public void setWithTableName(String withTableName) {
        this.withTableName = withTableName;
    }
}
