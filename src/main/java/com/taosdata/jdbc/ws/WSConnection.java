package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StmtUtils;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WSConnection extends AbstractConnection {
    private static final Logger log = LoggerFactory.getLogger(WSConnection.class);
    public static final AtomicBoolean g_FirstConnection = new AtomicBoolean(true);
    private final Transport transport;
    private final DatabaseMetaData metaData;
    private String database;
    private final ConnectionParam param;
    private final AtomicLong insertId = new AtomicLong(0);
    private final String jdbcUrl;

    private static final ConcurrentHashMap<String, ConCheckInfo> conCheckInfoMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> jdbcUrlLocks = new ConcurrentHashMap<>();

    // PreparedStatement cache
    private final LinkedHashMap<StmtCacheKey, PreparedStatement> stmtCache;
    private int maxCacheSize;

    public WSConnection(String url, Properties properties, Transport transport, ConnectionParam param, String serverVersion) {
        super(properties, serverVersion);
        this.transport = transport;
        this.database = param.getDatabase();
        this.param = param;
        this.jdbcUrl = StringUtils.retainHostPortPart(url);
        this.metaData = new WSDatabaseMetaData(url, properties.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
        this.maxCacheSize = param.getStmtCacheSize();
        this.stmtCache = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null)
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        WSStatement statement = new WSStatement(transport, database, this, idGenerator.getAndIncrement());

        statementsMap.put(statement.getInstanceId(), statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null) {
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        }

        boolean efficientWritingSql = false;
        if (sql.startsWith("ASYNC_INSERT")){
            sql = sql.substring("ASYNC_".length());
            efficientWritingSql = true;
        }

        if (!sql.contains("?")){
            AbsWSPreparedStatement stmt = new AbsWSPreparedStatement(transport,
                    param,
                    database,
                    this,
                    sql,
                    idGenerator.getAndIncrement());
            stmt.markInUse();
            statementsMap.put(stmt.getInstanceId(), stmt);
            return stmt;
        }

        // Check cache for parameterized SQL
        StmtCacheKey cacheKey = new StmtCacheKey(sql, database);
        PreparedStatement cached = stmtCache.get(cacheKey);
        if (cached != null) {
            WSRetryableStmt cachedStmt = (WSRetryableStmt) cached;
            if (!cachedStmt.isInUse()) {
                // Cache hit and idle, reuse it (statement stays registered
                // in statementsMap while cached).
                cachedStmt.markInUse();
                cachedStmt.reopenFromCache();
                return cached;
            }
            // Cache hit but in use, fall through to create new statement
        }

        if (transport != null && !transport.isClosed()) {
            Stmt2PrepareResp prepareResp = StmtUtils.initStmtWithRetry(transport, sql, param.getRetryTimes());

            boolean isInsert = prepareResp.isInsert();
            boolean isSuperTable = false;
            if (isInsert){
                for (Field field : prepareResp.getFields()){
                    if (field.getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                        isSuperTable = true;
                        break;
                    }
                }
            }

            if ((efficientWritingSql || "STMT".equalsIgnoreCase(param.getAsyncWrite())) && isInsert && isSuperTable) {
                if (supportsStmt2BindExec()) {
                    WSEWColumnPreparedStatement stmt = new WSEWColumnPreparedStatement(transport,
                            param,
                            database,
                            this,
                            sql,
                            idGenerator.getAndIncrement(),
                            prepareResp);
                    stmt.markInUse();
                    statementsMap.put(stmt.getInstanceId(), stmt);
                    return stmt;
                }
                WSEWPreparedStatement stmt = new WSEWPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
                stmt.markInUse();
                statementsMap.put(stmt.getInstanceId(), stmt);
                return stmt;
            } else {
                // Route insert statements to the stmt2 bind-exec producer on supported servers.
                if (isInsert && supportsStmt2BindExec()) {
                    WSColumnPreparedStatement stmt = new WSColumnPreparedStatement(transport,
                            param,
                            database,
                            this,
                            sql,
                            idGenerator.getAndIncrement(),
                            prepareResp);
                    stmt.markInUse();
                    statementsMap.put(stmt.getInstanceId(), stmt);
                    return stmt;
                }
                AbsWSPreparedStatement stmt = new AbsWSPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
                stmt.markInUse();
                statementsMap.put(stmt.getInstanceId(), stmt);
                return stmt;
            }
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Override
    public void close() throws SQLException {
        // Disable cache first
        maxCacheSize = 0;

        // Release idle cached statements first: they stay registered in
        // statementsMap, and their close() becomes a no-op once released here.
        releaseCachedStatements();

        // Close all registered statements (cached ones already released above,
        // close() sees isClosed() and returns; others take the original path).
        for (Map.Entry<Long, Statement> entry : statementsMap.entrySet()) {
            Statement value = entry.getValue();
            value.close();
        }
        statementsMap.clear();

        transport.close();
    }

    @Override
    public boolean canRebalanced() {
        // Idle cached statements do not block rebalance: they are released by
        // releaseCachedStatements() before the transport switches endpoints.
        for (Statement stmt : statementsMap.values()) {
            if (!(stmt instanceof WSRetryableStmt) || ((WSRetryableStmt) stmt).isInUse()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Release all cached statements. Called on connection close, and before a
     * rebalance switch while the old endpoint is still connected (so the
     * STMT2_CLOSE frames still reach the right server).
     */
    @Override
    public void releaseCachedStatements() {
        for (PreparedStatement stmt : stmtCache.values()) {
            try {
                ((WSRetryableStmt) stmt).releaseServerResource();
            } catch (SQLException e) {
                // Keep releasing the rest: a failure here must not leak the
                // remaining statements or abort connection close.
                log.error("Failed to release cached statement, sql: {}", ((WSRetryableStmt) stmt).getSql(), e);
            }
        }
        stmtCache.clear();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return transport.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return this.metaData;
    }
    public ConnectionParam getParam() {
        return param;
    }

    @Override
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        for (String line : lines) {
            CommonResp response = (CommonResp) transport.send(buildSchemalessInsertRequest(line, protocolType, timestampType, ttl, reqId), param.getRequestTimeout());
            if (Code.SUCCESS.getCode() != response.getCode()) {
                throw new SQLException("0x" + Integer.toHexString(response.getCode()) + ":" + response.getMessage());
            }
        }
    }

    /**
     * Writes schemaless payload without blocking the caller for the server ack.
     * The payload may contain multiple lines separated by {@code \n}, same as {@link #writeRaw}.
     *
     * @return a future completed with the insert response, or exceptionally with {@link SQLException}
     */
    public CompletableFuture<Response> writeAsync(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) {
        return writeAsync(line, protocolType, timestampType, null, null);
    }

    /**
     * Writes schemaless payload without blocking the caller for the server ack.
     *
     * @return a future completed with the insert response, or exceptionally with {@link SQLException}
     */
    public CompletableFuture<Response> writeAsync(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) {
        return transport.sendAsync(buildSchemalessInsertRequest(line, protocolType, timestampType, ttl, reqId), param.getRequestTimeout())
                .thenApply(response -> {
                    CommonResp commonResp = (CommonResp) response;
                    if (Code.SUCCESS.getCode() != commonResp.getCode()) {
                        throw new CompletionException(new SQLException("(0x" + Integer.toHexString(commonResp.getCode()) + "):" + commonResp.getMessage()));
                    }
                    return response;
                });
    }

    @Override
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        CommonResp response = (CommonResp) transport.send(buildSchemalessInsertRequest(line, protocolType, timestampType, ttl, reqId), param.getRequestTimeout());
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
        }
        // websocket don't return the num of schemaless insert
        return 0;
    }

    private Request buildSchemalessInsertRequest(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) {
        InsertReq insertReq = new InsertReq();
        insertReq.setReqId(insertId.getAndIncrement());
        insertReq.setProtocol(protocolType.ordinal());
        insertReq.setPrecision(timestampType.getType());
        insertReq.setData(line);
        if (ttl != null) {
            insertReq.setTtl(ttl);
        }
        if (reqId != null) {
            insertReq.setReqId(reqId);
        }
        return new Request(SchemalessAction.INSERT.getAction(), insertReq);
    }

    @Override
    public void setTimezone(String timezone) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        ZoneId zoneId = ConnectionParam.resolveTimezone(timezone);
        String normalizedTz = (timezone == null || timezone.isEmpty()) ? null : timezone;

        OptionsConnectionReq req = OptionsConnectionReq.ofTimezone(normalizedTz);
        CommonResp resp = (CommonResp) transport.send(new Request(Action.OPTIONS_CONNECTION.getAction(), req), param.getRequestTimeout());
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw TSDBError.createSQLException(resp.getCode(), resp.getMessage());
        }

        // update in place so that reconnects and new statements pick up the new timezone
        param.setTz(normalizedTz == null ? "" : normalizedTz);
        param.setZoneId(zoneId);
    }

    @Override
    public String getTimezone() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        String tz = param.getTz();
        return (tz == null || tz.isEmpty()) ? null : tz;
    }

    private boolean noNeedCheck(){
        ConCheckInfo conCheckInfo = conCheckInfoMap.get(jdbcUrl);
        return conCheckInfo != null
                && conCheckInfo.isValid()
                && !transport.isConnectionLost()
                && (!conCheckInfo.isExpired(param.getWsKeepAlive()));
    }
    @Override
    public boolean isValid(int timeout) throws SQLException {
        //true if the connection is valid, false otherwise
        if (isClosed())
            return false;
        if (timeout < 0)    //SQLException - if the value supplied for timeout is less than 0
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        if (noNeedCheck()){
            return true;
        }

        Object lock = jdbcUrlLocks.computeIfAbsent(jdbcUrl, k -> new Object());
        synchronized (lock) {
            if (noNeedCheck()){
                return true;
            }

            int status;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = createStatement();
                stmt.setQueryTimeout(timeout);
                resultSet = stmt.executeQuery("SHOW CLUSTER ALIVE");
                resultSet.next();
                status = resultSet.getInt(1);
                conCheckInfoMap.put(jdbcUrl, new ConCheckInfo(System.currentTimeMillis(), status != 0));
                return status != 0;
            } catch (SQLException e) {
                conCheckInfoMap.put(jdbcUrl, new ConCheckInfo(System.currentTimeMillis(), false));
                log.error("check connection failed", e);
                return false;
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }

    public boolean supportsStmt2BindExec() {
        String stmt2BindMode = param.getStmt2BindMode();
        // stmtBindMode is a routing override: column/traditional intentionally bypass version gating.
        if (ConnectionParam.STMT2_BIND_MODE_COLUMN.equalsIgnoreCase(stmt2BindMode)) {
            return true;
        }
        if (ConnectionParam.STMT2_BIND_MODE_TRADITIONAL.equalsIgnoreCase(stmt2BindMode)) {
            return false;
        }
        return this.supportStmt2BindExec;
    }

    // PreparedStatement cache management

    /**
     * Try to cache the given statement. Returns true if the statement is now
     * cached (and must stay alive); false if the caller should run the
     * original close path ({@code super.close()} + STMT2_CLOSE).
     */
    boolean tryCache(PreparedStatement stmt) throws SQLException {
        WSRetryableStmt retryable = (WSRetryableStmt) stmt;

        // Cache disabled (connection closing or stmtCacheSize=0)
        if (maxCacheSize <= 0) {
            return false;
        }

        // Only cache insert statements
        if (!retryable.getStmtInfo().isInsert()) {
            return false;
        }

        // Only cache the sql with ?
        if (retryable.getStmtInfo().getFields() == null || retryable.getStmtInfo().getFields().isEmpty()) {
            return false;
        }

        StmtCacheKey key = new StmtCacheKey(retryable.getStmtInfo().getSql(), retryable.getDatabase());
        PreparedStatement existing = stmtCache.get(key);

        // Already in cache: reset per-use state (parameters bound but never
        // executed must not linger) and mark idle.
        if (existing == stmt) {
            retryable.resetForReuse();
            retryable.markIdle();
            return true;
        }

        // Same SQL already cached by different statement, caller closes this one
        if (existing != null) {
            return false;
        }

        // Cache full, evict eldest (LRU)
        if (stmtCache.size() >= maxCacheSize) {
            java.util.Iterator<Map.Entry<StmtCacheKey, PreparedStatement>> it = stmtCache.entrySet().iterator();
            if (it.hasNext()) {
                Map.Entry<StmtCacheKey, PreparedStatement> eldest = it.next();
                WSRetryableStmt eldestStmt = (WSRetryableStmt) eldest.getValue();
                it.remove();
                // Release the evicted statement's server resource if not in use.
                if (!eldestStmt.isInUse()) {
                    eldestStmt.releaseServerResource();
                }
            }
        }

        // Add to cache
        retryable.resetForReuse();
        retryable.markIdle();
        stmtCache.put(key, stmt);
        return true;
    }

    static class StmtCacheKey {
        final String sql;
        final String database;

        StmtCacheKey(String sql, String database) {
            this.sql = sql;
            this.database = database;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StmtCacheKey)) return false;
            StmtCacheKey that = (StmtCacheKey) o;
            return sql.equals(that.sql) && java.util.Objects.equals(database, that.database);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql, database);
        }
    }
}
