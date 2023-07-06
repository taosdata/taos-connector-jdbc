package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.ws.entity.Payload;

public class SubscribeReq extends Payload {
    private String user;
    private String password;
    private String db;
    @JSONField(name = "group_id")
    private String groupId;
    @JSONField(name = "client_id")
    private String clientId;
    @JSONField(name = "offset_rest")
    private String offsetRest;
    private String[] topics;

    @JSONField(name = "auto_commit")
    private String autoCommit;
    @JSONField(name = "auto_commit_interval_ms")
    private String autoCommitIntervalMs;
    @JSONField(name = "snapshot_enable")
    private String snapshotEnable;
    @JSONField(name = "with_table_name")
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

    public String getSnapshotEnable() {
        return snapshotEnable;
    }

    public void setSnapshotEnable(String snapshotEnable) {
        this.snapshotEnable = snapshotEnable;
    }

    public String getWithTableName() {
        return withTableName;
    }

    public void setWithTableName(String withTableName) {
        this.withTableName = withTableName;
    }
}
