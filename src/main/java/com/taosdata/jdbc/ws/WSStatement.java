package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;
import com.taosdata.jdbc.ws.entity.*;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.taosdata.jdbc.utils.SqlSyntaxValidator.getDatabaseName;

public class WSStatement extends AbstractStatement {
    private final Transport transport;
    private String database;
    private final Connection connection;

    private boolean closed;
    private ResultSet resultSet;

    private int queryTimeout = 0;

    public WSStatement(Transport transport, String database, Connection connection) {
        this.transport = transport;
        this.database = database;
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        this.execute(sql, reqId);
        return this.resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return this.executeUpdate(sql, (Long) null);
    }

    public int executeUpdate(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        this.execute(sql, reqId);
        return affectedRows;
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            this.closed = true;
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return this.execute(sql, (Long) null);
    }

    public boolean execute(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if(sql != null){
            String out = "";
            StackTraceElement[] stackElements = Thread.currentThread().getStackTrace();
            if(stackElements != null)
            {
                for(int i = 0; i < stackElements.length; i++)
                {
                    out = out + stackElements[i] + '\n';
                }
            }

            try (FileWriter fileWriter = new FileWriter("./1234567out.txt", true)) {
                fileWriter.append(out);
                fileWriter.append(sql);
                fileWriter.append("----------------------------------------------------------\n");
                fileWriter.flush();
            } catch (IOException e) {
            }
       }

        if (null == reqId)
            reqId = ReqId.getReqID();
        Request request = RequestFactory.generateQuery(sql, reqId);
        Response response = transport.send(request);

        QueryResp queryResp = (QueryResp) response;
        if (Code.SUCCESS.getCode() != queryResp.getCode()) {
            throw TSDBError.createSQLException(queryResp.getCode(), queryResp.getMessage());
        }
        if (SqlSyntaxValidator.isUseSql(sql)) {
            this.database = getDatabaseName(sql);
            this.connection.setCatalog(this.database);
            this.connection.setClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME, this.database);
        }
        if (queryResp.isUpdate()) {
            this.resultSet = null;
            this.affectedRows = queryResp.getAffectedRows();
            return false;
        } else {
            this.resultSet = new BlockResultSet(this, this.transport, queryResp, this.database);
            this.affectedRows = -1;
            return true;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (seconds < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        this.queryTimeout = seconds;
        transport.setTimeout(seconds * 1000L);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return affectedRows;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
}
