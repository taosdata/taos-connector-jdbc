package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;

/**
 * Unit tests for {@link WSConnection#prepareStatement(String)} routing decisions.
 *
 * <p>These tests verify that the correct PreparedStatement implementation is chosen
 * based on server version and connection parameters, without requiring a live server.
 * Transport is stubbed with Mockito to return synthetic prepare responses.
 */
public class WSConnectionRoutingTest {

    private Transport transport;
    private ConnectionParam param;

    @Before
    public void setUp() {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);

        // Common stubs required by WSStatement / WSRetryableStmt constructors
        Mockito.when(transport.getConnectionParam()).thenReturn(param);
        Mockito.when(transport.getReconnectCount()).thenReturn(0);
        Mockito.when(transport.isConnected()).thenReturn(true);
        Mockito.when(transport.isClosed()).thenReturn(false);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        Mockito.when(param.getZoneId()).thenReturn(null);
        Mockito.when(param.getRetryTimes()).thenReturn(1);
        Mockito.when(param.getDatabase()).thenReturn("testdb");
    }

    // -----------------------------------------------------------------------
    // Helper factories
    // -----------------------------------------------------------------------

    /**
     * Creates a WSConnection whose server version is set to the given string.
     * The AbstractConnection constructor uses the version to compute cached capability flags.
     */
    private WSConnection makeConnection(String serverVersion) {
        Properties props = new Properties();
        return new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                props, transport, param, serverVersion);
    }

    /**
     * Configures Transport.send() mocks to simulate a successful stmt2 prepare
     * for an insert statement with a single timestamp column.
     */
    private void stubInsertPrepare() throws Exception {
        stubInsertPrepare(false);
    }

    private void stubInsertPrepare(boolean supertable) throws Exception {
        // Init response (stmt2 init action)
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(0);
        initResp.setStmtId(42L);

        // Prepare response with one timestamp COL field
        Stmt2PrepareResp prepResp = makeInsertPrepareResp(supertable);

        // send(Request) → Stmt2Resp
        Mockito.when(transport.send(any(Request.class))).thenReturn(initResp);
        // send(Request, boolean, long) → Stmt2PrepareResp
        Mockito.when(transport.send(any(Request.class), anyBoolean(), anyLong()))
                .thenReturn(prepResp);
    }

    /**
     * Configures Transport.send() mocks to simulate a successful stmt2 prepare
     * for a non-insert (query) statement.
     */
    private void stubQueryPrepare() throws Exception {
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(0);
        initResp.setStmtId(43L);

        Stmt2PrepareResp prepResp = makeInsertPrepareResp(false);
        prepResp.setInsert(false);

        Mockito.when(transport.send(any(Request.class))).thenReturn(initResp);
        Mockito.when(transport.send(any(Request.class), anyBoolean(), anyLong()))
                .thenReturn(prepResp);
    }

    private static Stmt2PrepareResp makeInsertPrepareResp(boolean supertable) {
        List<Field> fields = new ArrayList<>();
        if (supertable) {
            Field tbName = new Field();
            tbName.setBindType((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue());
            tbName.setFieldType((byte) TSDB_DATA_TYPE_INT);
            fields.add(tbName);
        }
        Field ts = new Field();
        ts.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        ts.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        ts.setPrecision((byte) 0);
        fields.add(ts);

        Field val = new Field();
        val.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        val.setFieldType((byte) TSDB_DATA_TYPE_INT);
        fields.add(val);

        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setCode(0);
        resp.setStmtId(42L);
        resp.setInsert(true);
        resp.setFields(fields);
        return resp;
    }

    // -----------------------------------------------------------------------
    // Test: insert + bind-exec server + default/null stmt2BindMode
    //       → WSColumnFastPreparedStatement
    // -----------------------------------------------------------------------

    @Test
    public void insert_bindExecServer_defaultStmt2BindMode_returnsWSColumnFastPreparedStatement() throws Exception {
        stubInsertPrepare();
        // pbsMode is NOT "line"
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        // Server version >= MIN_STMT2_BIND_EXEC_VERSION → supportsStmt2BindExec() returns true
        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSColumnFastPreparedStatement for insert on bind-exec server, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnFastPreparedStatement);
        }
    }

    @Test
    public void insert_bindExecServer_fastStmt2BindMode_returnsWSColumnFastPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSColumnFastPreparedStatement for stmt2BindMode=fast, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnFastPreparedStatement);
        }
    }

    @Test
    public void insert_bindExecServer_jdbcStmt2BindMode_returnsWSColumnFastPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSColumnFastPreparedStatement for stmt2BindMode=jdbc, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnFastPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: insert + old server → TSWSPreparedStatement (fallback)
    // -----------------------------------------------------------------------

    @Test
    public void insert_oldServer_returnsTSWSPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        // Server version < MIN_STMT2_BIND_EXEC_VERSION → supportsStmt2BindExec() returns false
        WSConnection conn = makeConnection("3.4.1.4");

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected TSWSPreparedStatement for insert on old server, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof TSWSPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: pbsMode=line no longer forces the removed row-mode implementation
    // -----------------------------------------------------------------------

    @Test
    public void insert_lineModeOnBindExecServer_returnsWSColumnFastPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSColumnFastPreparedStatement when pbsMode=line is ignored, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnFastPreparedStatement);
        }
    }

    @Test
    public void asyncInsert_bindExecServer_returnsWSEWColumnPreparedStatement() throws Exception {
        stubInsertPrepare(true);
        Mockito.when(param.getAsyncWrite()).thenReturn(null);
        Mockito.when(param.getBackendWriteThreadNum()).thenReturn(1);
        Mockito.when(param.getCacheSizeByRow()).thenReturn(1000);
        Mockito.when(param.getBatchSizeByRow()).thenReturn(100);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("ASYNC_INSERT INTO ? USING meters TAGS (?) VALUES (?,?)")) {
            assertNotNull(stmt);
            assertEquals(
                    WSEWColumnPreparedStatement.class,
                    stmt.getClass());
        }
    }

    @Test
    public void asyncInsert_oldServer_returnsLegacyWSEWPreparedStatement() throws Exception {
        stubInsertPrepare(true);
        Mockito.when(param.getAsyncWrite()).thenReturn(null);
        Mockito.when(param.getBackendWriteThreadNum()).thenReturn(1);
        Mockito.when(param.getCacheSizeByRow()).thenReturn(1000);
        Mockito.when(param.getBatchSizeByRow()).thenReturn(100);

        WSConnection conn = makeConnection("3.4.1.4");

        try (PreparedStatement stmt = conn.prepareStatement("ASYNC_INSERT INTO ? USING meters TAGS (?) VALUES (?,?)")) {
            assertNotNull(stmt);
            assertEquals(WSEWPreparedStatement.class, stmt.getClass());
        }
    }

    // -----------------------------------------------------------------------
    // Test: query SQL → TSWSPreparedStatement (write path not applicable)
    // -----------------------------------------------------------------------

    @Test
    public void query_bindExecServer_returnsTSWSPreparedStatement() throws Exception {
        stubQueryPrepare();
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM t WHERE id=?")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected TSWSPreparedStatement for query SQL, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof TSWSPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: insert with TAOS_FIELD_QUERY field + supported server → still WSColumnFastPreparedStatement
    // Verifies routing is version-based only; there is no shape gate for query fields.
    // -----------------------------------------------------------------------

    @Test
    public void insert_bindExecServer_withQueryField_stillRoutesToWSColumnFastPreparedStatement()
            throws Exception {
        // Build a prepare response that includes a TAOS_FIELD_QUERY field
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(0);
        initResp.setStmtId(44L);

        List<Field> fields = new ArrayList<>();
        Field queryField = new Field();
        queryField.setBindType((byte) FieldBindType.TAOS_FIELD_QUERY.getValue());
        queryField.setFieldType((byte) TSDB_DATA_TYPE_INT);
        queryField.setPrecision((byte) 0);
        fields.add(queryField);

        Field tsField = new Field();
        tsField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        tsField.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        tsField.setPrecision((byte) 0);
        fields.add(tsField);

        Stmt2PrepareResp prepResp = new Stmt2PrepareResp();
        prepResp.setCode(0);
        prepResp.setStmtId(44L);
        prepResp.setInsert(true);
        prepResp.setFields(fields);

        Mockito.when(transport.send(any(Request.class))).thenReturn(initResp);
        Mockito.when(transport.send(any(Request.class), anyBoolean(), anyLong()))
                .thenReturn(prepResp);

        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Routing must be version-based only; TAOS_FIELD_QUERY shape must not gate "
                            + "WSColumnFastPreparedStatement. Got: " + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnFastPreparedStatement);
        }
    }
}
