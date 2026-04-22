package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.entity.CommonResp;
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
     * The AbstractConnection constructor uses the version to compute supportBlob / supportLineBind.
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
        // Init response (stmt2 init action)
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(0);
        initResp.setStmtId(42L);

        // Prepare response with one timestamp COL field
        Stmt2PrepareResp prepResp = makeInsertPrepareResp(false);

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
    // Test: insert + bind-exec server → WSColumnPreparedStatement
    // -----------------------------------------------------------------------

    @Test
    public void insert_bindExecServer_returnsWSColumnPreparedStatement() throws Exception {
        stubInsertPrepare();
        // pbsMode is NOT "line"
        Mockito.when(param.getPbsMode()).thenReturn(null);
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        // Server version >= 3.1.4.10 → supportsStmt2BindExec() returns true
        WSConnection conn = makeConnection("3.3.0.0");

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSColumnPreparedStatement for insert on bind-exec server, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSColumnPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: insert + old server → TSWSPreparedStatement (fallback)
    // -----------------------------------------------------------------------

    @Test
    public void insert_oldServer_returnsTSWSPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getPbsMode()).thenReturn(null);
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        // Server version < 3.1.4.10 → supportsStmt2BindExec() returns false
        WSConnection conn = makeConnection("3.1.4.9");

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected TSWSPreparedStatement for insert on old server, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof TSWSPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: pbsMode=line + bind-exec server → WSRowPreparedStatement
    // -----------------------------------------------------------------------

    @Test
    public void insert_lineModeOnBindExecServer_returnsWSRowPreparedStatement() throws Exception {
        stubInsertPrepare();
        Mockito.when(param.getPbsMode()).thenReturn("line");
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        // Server version that supports both line-bind and bind-exec;
        // the "line" pbsMode takes priority → WSRowPreparedStatement.
        // supportLineBind is derived from VersionUtil.surpportBlob() which requires >= 3.3.7.0.alpha.
        // Use "3.3.7.0" which satisfies that constraint.
        WSConnection conn = makeConnection("3.3.7.0");

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?,?)")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected WSRowPreparedStatement for pbsMode=line, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof WSRowPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // Test: query SQL → TSWSPreparedStatement (write path not applicable)
    // -----------------------------------------------------------------------

    @Test
    public void query_bindExecServer_returnsTSWSPreparedStatement() throws Exception {
        stubQueryPrepare();
        Mockito.when(param.getPbsMode()).thenReturn(null);
        Mockito.when(param.getAsyncWrite()).thenReturn(null);

        WSConnection conn = makeConnection("3.3.0.0");

        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM t WHERE id=?")) {
            assertNotNull(stmt);
            assertTrue(
                    "Expected TSWSPreparedStatement for query SQL, got: "
                            + stmt.getClass().getSimpleName(),
                    stmt instanceof TSWSPreparedStatement);
        }
    }
}
