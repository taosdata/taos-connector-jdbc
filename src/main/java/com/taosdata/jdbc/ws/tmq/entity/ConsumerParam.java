package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.util.Properties;

public class ConsumerParam {
    private ConnectionParam connectionParam;
    private String groupId;
    private String clientId;
    private String offsetRest;
    private final boolean autoCommit;
    private String autoCommitInterval;
    private String msgWithTableName;

    public ConsumerParam(Properties properties) throws SQLException {
        if (null != properties.getProperty(TMQConstants.CONNECT_URL)) {
            String url = properties.getProperty(TMQConstants.CONNECT_URL);
            StringUtils.parseUrl(url, properties);
        }
        if (null != properties.getProperty(TMQConstants.CONNECT_USER))
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, properties.getProperty(TMQConstants.CONNECT_USER));
        if (null != properties.getProperty(TMQConstants.CONNECT_PASS))
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, properties.getProperty(TMQConstants.CONNECT_PASS));
        if (null != properties.getProperty(TMQConstants.CONNECT_IP))
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, properties.getProperty(TMQConstants.CONNECT_IP));
        if (null != properties.getProperty(TMQConstants.CONNECT_PORT))
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PORT, properties.getProperty(TMQConstants.CONNECT_PORT));
        autoCommit = Boolean.parseBoolean(properties.getProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true"));
        connectionParam = ConnectionParam.getParam(properties);
        groupId = properties.getProperty(TMQConstants.GROUP_ID);
        clientId = properties.getProperty(TMQConstants.CLIENT_ID);
        offsetRest = properties.getProperty(TMQConstants.AUTO_OFFSET_RESET);
        autoCommitInterval = properties.getProperty(TMQConstants.AUTO_COMMIT_INTERVAL);
        msgWithTableName = properties.getProperty(TMQConstants.MSG_WITH_TABLE_NAME);
    }

    public ConnectionParam getConnectionParam() {
        return connectionParam;
    }

    public void setConnectionParam(ConnectionParam connectionParam) {
        this.connectionParam = connectionParam;
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

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public String getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(String autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public String getMsgWithTableName() {
        return msgWithTableName;
    }

    public void setMsgWithTableName(String msgWithTableName) {
        this.msgWithTableName = msgWithTableName;
    }
}
