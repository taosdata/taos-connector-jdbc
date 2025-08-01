package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.VersionUtil;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractConnection extends WrapperImpl implements Connection {

    protected  AtomicLong idGenerator = new AtomicLong(0);
    protected ConcurrentHashMap<Long, Statement> statementsMap = new ConcurrentHashMap<>();


    protected volatile boolean isClosed;
    protected volatile String catalog;
    protected final Properties clientInfoProps = new Properties();
    protected final boolean supportBlob;
    protected final boolean supportLineBind;

    protected final String serverVersion;

    protected AbstractConnection(Properties properties, String version) {
        Set<String> propNames = properties.stringPropertyNames();
        for (String propName : propNames) {
            clientInfoProps.setProperty(propName, properties.getProperty(propName));
        }
        serverVersion = version;
        supportBlob = VersionUtil.surpportBlob(serverVersion);
        supportLineBind = supportBlob;
    }

    public boolean isSupportBlob(){return supportBlob;}
    public void unregisterStatement(Long stmtId) {
        this.statementsMap.remove(stmtId);
    }

    public void registerStatement(Long stmtId, Statement stmt) {
        this.statementsMap.put(stmtId, stmt);
    }

    @Override
    public abstract Statement createStatement() throws SQLException;

    @Override
    public abstract PreparedStatement prepareStatement(String sql) throws SQLException;

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        // do nothing

        return sql;
    }


    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        //do nothing
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return true;
    }

    @Override
    public void commit() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        // do nothing
    }

    @Override
    public void rollback() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        // do nothing
    }

    @Override
    public abstract void close() throws SQLException;

    @Override
    public abstract boolean isClosed() throws SQLException;

    @Override
    public abstract DatabaseMetaData getMetaData() throws SQLException;

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        //do nothing
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return this.catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        switch (level) {
            case Connection.TRANSACTION_NONE:
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }
        //do nothing
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        // do nothing
    }

    private void checkResultSetTypeAndResultSetConcurrency(int resultSetType, int resultSetConcurrency) throws SQLException {
        switch (resultSetType) {
            case ResultSet.TYPE_FORWARD_ONLY:
                break;
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }

        switch (resultSetConcurrency) {
            case ResultSet.CONCUR_READ_ONLY:
                break;
            case ResultSet.CONCUR_UPDATABLE:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        checkResultSetTypeAndResultSetConcurrency(resultSetType, resultSetConcurrency);
        return createStatement();
    }
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        checkResultSetTypeAndResultSetConcurrency(resultSetType, resultSetConcurrency);
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        switch (holdability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }
        //do nothing
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        switch (resultSetHoldability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        switch (resultSetHoldability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        switch (autoGeneratedKeys) {
            case Statement.RETURN_GENERATED_KEYS:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
            case Statement.NO_GENERATED_KEYS:
                break;
        }
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Clob createClob() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Blob createBlob() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public NClob createNClob() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        //true if the connection is valid, false otherwise
        if (isClosed())
            return false;
        if (timeout < 0)    //SQLException - if the value supplied for timeout is less than 0
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<Boolean> future = executor.submit(() -> {
            int status;
            try (Statement stmt = createStatement()) {
                ResultSet resultSet = stmt.executeQuery("select 1");
                resultSet.next();
                status = resultSet.getInt("1");
                resultSet.close();
            }
            return status == 1;
        });

        boolean status = false;
        try {
            if (timeout == 0)
                status = future.get();
            else
                status = future.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException ignored) {
        } catch (TimeoutException e) {
            future.cancel(true);
        } finally {
            executor.shutdownNow();
        }
        return status;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (isClosed)
            throw (SQLClientInfoException) TSDBError.createSQLException(TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED);

        clientInfoProps.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (isClosed)
            throw (SQLClientInfoException) TSDBError.createSQLException(TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED);

        for (Enumeration<Object> enumeration = properties.keys(); enumeration.hasMoreElements(); ) {
            String name = (String) enumeration.nextElement();
            clientInfoProps.put(name, properties.getProperty(name));
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        if (isClosed)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return clientInfoProps.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        if (isClosed)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return clientInfoProps;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        //do nothing
    }

    @Override
    public String getSchema() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        // do nothing
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        if (milliseconds < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        // do nothing
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return 0;
    }


    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException{
        write(lines, protocolType, timestampType, null, null);
    }

    public abstract void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException;


    /**
     * only one line writes to db
     *
     * @param line          schemaless line
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType)throws SQLException{
        write(new String[]{line}, protocolType, timestampType);
    }

    /**
     * batch schemaless lines write to db with list
     *
     * @param lines         schemaless list
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(List<String> lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException{
        String[] strings = lines.toArray(new String[0]);
        write(strings, protocolType, timestampType);
    }

    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException{
        return writeRaw(line, protocolType, timestampType, null, null);
    }

    public abstract int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException;
}
