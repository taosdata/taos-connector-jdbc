/*
 * Copyright (c) 2020 TAOS Data, Inc.
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

package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulConnection extends AbstractConnection {

    private final String host;
    private final String port;
    private final String url;
    private final String database;
    private final String auth;
    private final boolean useSsl;
    private final String token;
    private final String tz;
    private final DatabaseMetaData metadata;

    public RestfulConnection(String host, String port, Properties props, String database, String url, String auth, boolean useSsl, String token, String tz) {
        super(props, TSDBConstants.UNKNOWN_VERSION);
        this.host = host;
        this.port = port;
        this.database = database;
        this.url = url;
        this.auth = auth == null ? null : "Basic " + auth;
        this.useSsl = useSsl;
        this.token = token;
        this.tz = tz;
        this.metadata = new RestfulDatabaseMetaData(url, props.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
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
        // TODO: release all resources
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

    @Override
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTimezone(String timezone) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                "setTimezone is only supported on WebSocket connections");
    }

    @Override
    public String getTimezone() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                "getTimezone is only supported on WebSocket connections");
    }
    // getters
    public String getHost() {
        return host;
    }

    public String getPort() {
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

    public String getTz() {
        return tz;
    }
}