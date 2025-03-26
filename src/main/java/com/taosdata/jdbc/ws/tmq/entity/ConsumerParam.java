package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class ConsumerParam {
    private static final HashSet<String> knownKeys = new HashSet<>();
    static {

        knownKeys.add(TSDBDriver.PROPERTY_KEY_USER);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_PASSWORD);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_HOST);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_PORT);

        knownKeys.add(TMQConstants.CONNECT_USER);
        knownKeys.add(TMQConstants.CONNECT_PASS);
        knownKeys.add(TMQConstants.CONNECT_IP);
        knownKeys.add(TMQConstants.CONNECT_PORT);
        knownKeys.add(TMQConstants.ENABLE_AUTO_COMMIT);
        knownKeys.add(TMQConstants.GROUP_ID);
        knownKeys.add(TMQConstants.CLIENT_ID);
        knownKeys.add(TMQConstants.AUTO_OFFSET_RESET);
        knownKeys.add(TMQConstants.AUTO_COMMIT_INTERVAL);
        knownKeys.add(TMQConstants.MSG_WITH_TABLE_NAME);
        knownKeys.add(TMQConstants.MSG_ENABLE_BATCH_META);
        knownKeys.add(TMQConstants.BOOTSTRAP_SERVERS);
        knownKeys.add(TMQConstants.VALUE_DESERIALIZER);
        knownKeys.add(TMQConstants.VALUE_DESERIALIZER_ENCODING);
        knownKeys.add(TMQConstants.CONNECT_TYPE);
        knownKeys.add(TMQConstants.CONNECT_URL);

        knownKeys.add(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION);

        knownKeys.add(TSDBDriver.PROPERTY_KEY_APP_IP);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_APP_NAME);
        knownKeys.add(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
    }

    private ConnectionParam connectionParam;
    private String groupId;
    private String clientId;
    private String offsetRest;
    private final boolean autoCommit;
    private long autoCommitInterval;
    private String msgWithTableName;
    private String enableBatchMeta;

    private HashMap<String, String> config = new HashMap<>();

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
        if (!StringUtils.isEmpty(connectionParam.getSlaveClusterHost())){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_ERROR, "slaveClusterHost is not supported in consumer param");
        }

        groupId = properties.getProperty(TMQConstants.GROUP_ID);
        clientId = properties.getProperty(TMQConstants.CLIENT_ID);
        offsetRest = properties.getProperty(TMQConstants.AUTO_OFFSET_RESET);
        autoCommitInterval = Long.parseLong(properties.getProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "5000"));
        if (autoCommitInterval < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_ERROR, "autoCommitInterval must be greater than 0");
        }


        msgWithTableName = properties.getProperty(TMQConstants.MSG_WITH_TABLE_NAME);
        enableBatchMeta = properties.getProperty(TMQConstants.MSG_ENABLE_BATCH_META, null);

        for (String key : properties.stringPropertyNames()) {
            if (!knownKeys.contains(key)) {
                config.put(key, properties.getProperty(key));
            }
        }
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

    public long getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(long autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public String getMsgWithTableName() {
        return msgWithTableName;
    }

    public void setMsgWithTableName(String msgWithTableName) {
        this.msgWithTableName = msgWithTableName;
    }

    public String getEnableBatchMeta() {
        return enableBatchMeta;
    }

    public void setEnableBatchMeta(String enableBatchMeta) {
        this.enableBatchMeta = enableBatchMeta;
    }

    public HashMap<String, String> getConfig() {
        return config;
    }

}
