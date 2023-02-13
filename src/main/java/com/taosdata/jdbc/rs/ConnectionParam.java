package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.ws.Transport;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionParam {
    private String host;
    private String port;
    private String database;
    private String cloudToken;
    private String user;
    private String password;
    private String tz;
    private boolean useSsl;
    private int maxRequest;
    private int connectTimeout;
    private int requestTimeout;

    private ConnectionParam(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.cloudToken = builder.cloudToken;
        this.user = builder.user;
        this.password = builder.password;
        this.tz = builder.tz;
        this.useSsl = builder.useSsl;
        this.maxRequest = builder.maxRequest;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCloudToken() {
        return cloudToken;
    }

    public void setCloudToken(String cloudToken) {
        this.cloudToken = cloudToken;
    }

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

    public String getTz() {
        return tz;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public int getMaxRequest() {
        return maxRequest;
    }

    public void setMaxRequest(int maxRequest) {
        this.maxRequest = maxRequest;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public static ConnectionParam getParam(Properties properties) throws SQLException {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String database = properties.containsKey(TSDBDriver.PROPERTY_KEY_DBNAME)
                ? properties.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME)
                : null;

        String cloudToken = null;
        if (properties.containsKey(TSDBDriver.PROPERTY_KEY_TOKEN)) {
            cloudToken = properties.getProperty(TSDBDriver.PROPERTY_KEY_TOKEN);
        }

        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER);
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD);

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
                    "unsupported UTF-8 concoding, user: " + properties.getProperty(TSDBDriver.PROPERTY_KEY_USER)
                            + ", password: " + properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
        }

        String tz = properties.getProperty(TSDBDriver.HTTP_TIME_ZONE);

        boolean useSsl = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_USE_SSL, "false"));

        int maxRequest = Integer
                .parseInt(properties.getProperty(TSDBDriver.HTTP_POOL_SIZE, HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
        int connectTimeout = Integer.parseInt(
                properties.getProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT));

        int requestTimeout = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT,
                String.valueOf(Transport.DEFAULT_MESSAGE_WAIT_TIMEOUT)));

        return new ConnectionParam.Builder(host, port).setDatabase(database).setCloudToken(cloudToken)
                .setUserAndPassword(user, password).setTimeZone(tz).setUseSsl(useSsl).setMaxRequest(maxRequest)
                .setConnectionTimeout(connectTimeout).setRequestTimeout(requestTimeout).build();
    }

    public static class Builder {
        private final String host;
        private final String port;
        private String database;
        private String cloudToken;
        private String user;
        private String password;
        private String tz;
        private boolean useSsl;
        private int maxRequest;
        private int connectTimeout;
        private int requestTimeout;

        public Builder(String host, String port) {
            this.host = host;
            this.port = port;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setCloudToken(String cloudToken) {
            this.cloudToken = cloudToken;
            return this;
        }

        public Builder setUserAndPassword(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder setTimeZone(String timeZone) {
            this.tz = timeZone;
            return this;
        }

        public Builder setUseSsl(boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        public Builder setMaxRequest(int maxRequest) {
            this.maxRequest = maxRequest;
            return this;
        }

        public Builder setConnectionTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public ConnectionParam build() {
            return new ConnectionParam(this);
        }
    }
}
