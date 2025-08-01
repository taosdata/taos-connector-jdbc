package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.AbstractDatabaseMetaData;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
@Deprecated
public class RestfulDatabaseMetaData extends AbstractDatabaseMetaData {

    private final String url;
    private final String userName;
    private final Connection connection;

    public RestfulDatabaseMetaData(String url, String userName, Connection connection) {
        this.url = url;
        this.userName = userName;
        this.connection = connection;
    }

    @Override
    public String getURL() throws SQLException {
        return this.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return this.userName;
    }

    @Override
    public String getDriverName() throws SQLException {
        return RestfulDriver.class.getName();
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (connection == null || connection.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return super.getTables(catalog, schemaPattern, tableNamePattern, types, connection);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getCatalogs(connection);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        if (connection == null || connection.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return super.getTableTypes();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern, connection);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getPrimaryKeys(catalog, schema, table, connection);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getSuperTables(catalog, schemaPattern, tableNamePattern, connection);
    }

}
