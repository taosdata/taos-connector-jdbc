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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TSDBDatabaseMetaData extends AbstractDatabaseMetaData {

    private final String url;
    private final String userName;
    private Connection conn;

    public TSDBDatabaseMetaData(String url, String userName) {
        this.url = url;
        this.userName = userName;
    }

    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public String getURL() throws SQLException {
        return this.url;
    }

    public String getUserName() throws SQLException {
        return this.userName;
    }

    public String getDriverName() throws SQLException {
        return TSDBDriver.class.getName();
    }

    /**
     * @Param catalog : database名称，"" 表示不属于任何database的table，null表示不使用database来缩小范围
     * @Param schemaPattern : schema名称，""表示
     * @Param tableNamePattern : 表名满足tableNamePattern的表, null表示返回所有表
     * @Param types : 表类型，null表示返回所有类型
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (conn == null || conn.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return super.getTables(catalog, schemaPattern, tableNamePattern, types, conn);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getCatalogs(conn);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getTableTypes();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern, conn);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getPrimaryKeys(catalog, schema, table, conn);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getSuperTables(catalog, schemaPattern, tableNamePattern, conn);
    }
}