/*
 * Copyright (c) 2019 TAOS Data, Inc.
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

package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

public class TSDBConnection extends AbstractConnection {
    private TSDBJNIConnector connector;
    private final TSDBDatabaseMetaData databaseMetaData;

    public TSDBConnection(Properties info, TSDBDatabaseMetaData meta) throws SQLException {
        super(info, TSDBConstants.UNKNOWN_VERSION);
        this.databaseMetaData = meta;
        connect(info.getProperty(TSDBDriver.PROPERTY_KEY_HOST),
                Integer.parseInt(info.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "0")),
                info.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME),
                info.getProperty(TSDBDriver.PROPERTY_KEY_USER),
                info.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
    }

    private void connect(String host, int port, String dbName, String user, String password) throws SQLException {
        this.connector = new TSDBJNIConnector();
        this.connector.connect(host, port, dbName, user, password);
        this.catalog = dbName;
        this.databaseMetaData.setConnection(this);
    }

    public TSDBJNIConnector getConnector() {
        return this.connector;
    }

    public Statement createStatement() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        return new TSDBStatement(this, idGenerator.getAndIncrement());
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return new TSDBPreparedStatement(this, sql, idGenerator.getAndIncrement());
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        synchronized (this) {
            if (isClosed) {
                return;
            }

            for (Map.Entry<Long, Statement> entry : statementsMap.entrySet()) {
                Statement value = entry.getValue();
                value.close();
            }
            statementsMap.clear();

            this.connector.closeConnection();
            this.isClosed = true;
        }
    }



    public boolean isClosed() throws SQLException {
        return this.connector != null && this.connector.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return this.databaseMetaData;
    }

    @Override
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        if (null == ttl && null == reqId) {
            connector.insertLines(lines, protocolType, timestampType);
        } else if (null == reqId) {
            connector.insertLinesWithTtl(lines, protocolType, timestampType, ttl);
        } else if (null == ttl) {
            connector.insertLinesWithReqId(lines, protocolType, timestampType, reqId);
        } else {
            connector.insertLinesWithTtlAndReqId(lines, protocolType, timestampType, ttl, reqId);
        }
    }
    @Override
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        if (null == ttl && null == reqId) {
            return connector.insertRaw(line, protocolType, timestampType);
        } else if (null == reqId) {
            return connector.insertRawWithTtl(line, protocolType, timestampType, ttl);
        } else if (null == ttl) {
            return connector.insertRawWithReqId(line, protocolType, timestampType, reqId);
        } else {
            return connector.insertRawWithTtlAndReqId(line, protocolType, timestampType, ttl, reqId);
        }
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
}