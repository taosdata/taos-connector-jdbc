package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.TimestampFormat;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RestfulConnection extends AbstractConnection {

    private final String host;
    private final int port;
    private final String url;
    private final String database;
    private final String auth;
    private final boolean useSsl;
    private final String token;

    private boolean isClosed;
    private final DatabaseMetaData metadata;

    public RestfulConnection(String host, String port, Properties props, String database, String url, String auth, boolean useSsl, String token) {
        super(props);
        this.host = host;
        this.port = Integer.parseInt(port);
        this.database = database;
        this.url = url;
        this.auth = "Basic " + auth;
        this.useSsl = useSsl;
        this.token = token;
        this.metadata = new RestfulDatabaseMetaData(url, props.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    /**
     * A convenient constructor for cloud user
     *
     * @param url   TDengine Cloud URL
     * @param token TDengine Cloud Token
     * @throws Exception
     */
    public RestfulConnection(String url, String token) throws Exception {
        super(new Properties());
        clientInfoProps.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, String.valueOf(TimestampFormat.TIMESTAMP));

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.HTTP_POOL_SIZE, HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE);
        properties.setProperty(TSDBDriver.HTTP_KEEP_ALIVE, "true");
        HttpClientPoolUtil.init(properties);
        if (url == null || token == null) {
            throw new Exception("url and token can't be null");
        }
        this.url = url;
        this.useSsl = url.startsWith("https://");
        String tmpSchema = this.useSsl ? "https://" : "http://";
        String hostPort = url.replaceFirst(tmpSchema, "");
        String[] splits = hostPort.split(":");
        this.host = splits[0];
        if (splits.length == 2) {
            this.port = Integer.parseInt(splits[1]);
        } else {
            this.port = this.useSsl ? 443 : 80;
        }
        this.database = null;
        this.auth = null;
        this.token = token;
        this.metadata = new RestfulDatabaseMetaData(this.url, null, this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return new RestfulStatement(this, database);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return new RestfulPreparedStatement(this, database, sql);
    }

    @Override
    public void close() throws SQLException {
        if (isClosed)
            return;
        //TODO: release all resources
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return this.metadata;
    }

    // getters
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUrl() {
        return url;
    }

    public String getToken() {
        return token;
    }

    public String getAuth() {
        return auth;
    }

    public boolean isUseSsl() {
        return useSsl;
    }
}