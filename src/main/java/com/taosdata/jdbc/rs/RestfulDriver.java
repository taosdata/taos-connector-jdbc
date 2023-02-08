package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.WSConnection;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RestfulDriver extends AbstractDriver {

    private static final String URL_PREFIX = "jdbc:TAOS-RS://";

    static {
        try {
            DriverManager.registerDriver(new RestfulDriver());
        } catch (SQLException e) {
            throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_URL_NOT_SET, e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // throw SQLException if url is null
        if (url == null || url.trim().isEmpty() || url.trim().replaceAll("\\s", "").isEmpty())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);

        // return null if url is not be accepted
        if (!acceptsURL(url))
            return null;

        Properties props = parseURL(url, info);
        String host = props.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = props.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String database = props.containsKey(TSDBDriver.PROPERTY_KEY_DBNAME)
                ? props.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME)
                : null;

        String cloudToken = null;
        if (props.containsKey(TSDBDriver.PROPERTY_KEY_TOKEN)) {
            cloudToken = props.getProperty(TSDBDriver.PROPERTY_KEY_TOKEN);
        }

        String user = props.getProperty(TSDBDriver.PROPERTY_KEY_USER);
        String password = props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD);

        if (user == null && cloudToken == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED);
        }
        if (password == null && cloudToken == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED);
        }

        try {
            if (user != null) {
                user = URLDecoder.decode(user, StandardCharsets.UTF_8.displayName());
            }
            if (password != null) {
                password = URLDecoder.decode(password, StandardCharsets.UTF_8.displayName());
            }
        } catch (UnsupportedEncodingException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                    "unsupported UTF-8 concoding, user: " + props.getProperty(TSDBDriver.PROPERTY_KEY_USER)
                            + ", password: " + props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
        }

        String tz = props.getProperty(TSDBDriver.HTTP_TIME_ZONE);

        boolean useSsl = Boolean.parseBoolean(props.getProperty(TSDBDriver.PROPERTY_KEY_USE_SSL, "false"));
        String loginUrl;
        String batchLoad = info.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD);
        if (Boolean.parseBoolean(batchLoad)) {
            String protocol = "ws";
            if (useSsl) {
                protocol = "wss";
            }
            loginUrl = protocol + "://" + props.getProperty(TSDBDriver.PROPERTY_KEY_HOST)
                    + ":" + props.getProperty(TSDBDriver.PROPERTY_KEY_PORT) + "/rest/ws";
            if (null != cloudToken) {
                loginUrl = loginUrl + "?token=" + cloudToken;
            }
            WSClient client = null;
            Transport transport = null;

            int timeout = Integer.parseInt(props.getProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT,
                    String.valueOf(Transport.DEFAULT_MESSAGE_WAIT_TIMEOUT)));
            int maxRequest = Integer
                    .parseInt(props.getProperty(TSDBDriver.HTTP_POOL_SIZE, HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
            int connectTimeout = Integer.parseInt(
                    props.getProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT));

            InFlightRequest inFlightRequest = new InFlightRequest(timeout, maxRequest);
            CountDownLatch latch = new CountDownLatch(1);
            Map<String, String> httpHeaders = new HashMap<>();
            URI urlPath = null;
            try {
                urlPath = new URI(loginUrl);
            } catch (URISyntaxException e) {
                throw new SQLException("Websocket url parse error: " + loginUrl, e);
            }
            client = new WSClient(urlPath, user, password, database,
                    inFlightRequest, httpHeaders, latch, maxRequest);
            transport = new Transport(client, inFlightRequest, timeout);
            try {
                if (!client.connectBlocking(connectTimeout, TimeUnit.MILLISECONDS)) {
                    close(transport);
                    throw new SQLException("can't create connection with server");
                }
                if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    close(transport);
                    throw new SQLException("auth timeout");
                }
            } catch (InterruptedException e) {
                close(transport);
                throw new SQLException("create websocket connection has been Interrupted ", e);
            }
            if (!client.isAuth()) {
                close(transport);
                throw new SQLException("auth failure");
            }
            TaosGlobalConfig.setCharset(props.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET));
            return new WSConnection(url, props, transport, database);
        }
        HttpClientPoolUtil.init(props);

        String auth = null;

        if (user != null && password != null) {
            auth = Base64.getEncoder().encodeToString(
                    (user + ":" + password).getBytes(StandardCharsets.UTF_8));
        }

        RestfulConnection conn = new RestfulConnection(host, port, props, database, url, auth, useSsl, cloudToken, tz);
        if (database != null && !database.trim().replaceAll("\\s", "").isEmpty()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + database);
            }
        }
        return conn;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);
        return url.trim().length() > 0 && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        if (acceptsURL(url)) {
            info = parseURL(url, info);
        }
        return getPropertyInfo(info);
    }

    @Override
    public int getMajorVersion() {
        return 3;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO SQLFeatureNotSupportedException
        throw new SQLFeatureNotSupportedException();
    }

    private void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // handle exception
            }
        }
    }
}
