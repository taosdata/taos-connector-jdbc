package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link WSColumnPreparedStatement}.
 *
 * <p>These tests exercise the row-staging machinery (last-setter-wins,
 * addBatch flush, null handling) without requiring a live server.
 * Transport and connection dependencies are stubbed via Mockito.
 *
 * <p>The test is in the same package as the production class so that
 * package-private accessors ({@code getColumnBuffer}, {@code getExpectedRowCount},
 * {@code getTbNameFieldIdx}) are accessible.
 */
public class WSColumnPreparedStatementTest {

    private Transport transport;
    private ConnectionParam param;
    private AbstractConnection connection;

    @Before
    public void setUp() {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);
        connection = Mockito.mock(AbstractConnection.class);

        Mockito.when(transport.getConnectionParam()).thenReturn(param);
        Mockito.when(transport.getReconnectCount()).thenReturn(0);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        Mockito.when(param.getZoneId()).thenReturn(null);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Stmt2PrepareResp makeInsertPrepareResp(List<Field> fields) {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(true);
        resp.setStmtId(1L);
        resp.setFields(fields);
        return resp;
    }

    private static Field makeField(byte bindType, byte fieldType, byte precision) {
        Field f = new Field();
        f.setBindType(bindType);
        f.setFieldType(fieldType);
        f.setPrecision(precision);
        return f;
    }

    private WSColumnPreparedStatement buildStmt(List<Field> fields) {
        Stmt2PrepareResp resp = makeInsertPrepareResp(fields);
        return new WSColumnPreparedStatement(transport, param, "test_db",
                connection, "INSERT INTO t VALUES (?)", 1L, resp);
    }

    private static int bufRowCount(WSColumnPreparedStatement stmt, int fieldIdx) {
        return stmt.getColumnBuffer(fieldIdx).getRowCount();
    }

    // -----------------------------------------------------------------------
    // last-setter-wins – fixed-width
    // -----------------------------------------------------------------------

    @Test
    public void repeatedSetInt_lastValueWins_singleRowFlushed() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setInt(1, 10);
        stmt.setInt(1, 20);
        stmt.setInt(1, 30);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, stmt.getExpectedRowCount());
    }

    // -----------------------------------------------------------------------
    // last-setter-wins – variable-width
    // -----------------------------------------------------------------------

    @Test
    public void repeatedSetString_lastValueWins_singleRowFlushed() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setString(1, "alpha");
        stmt.setString(1, "beta");
        stmt.setString(1, "gamma");
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, stmt.getExpectedRowCount());
    }

    // -----------------------------------------------------------------------
    // addBatch accumulates multiple rows
    // -----------------------------------------------------------------------

    @Test
    public void addBatch_multipleRows_allFieldBuffersAdvanced() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_BIGINT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        long now = System.currentTimeMillis();
        for (int i = 0; i < 7; i++) {
            stmt.setTimestamp(1, new Timestamp(now + i));
            stmt.setLong(2, 1000L + i);
            stmt.addBatch();
        }

        assertEquals(7, stmt.getExpectedRowCount());
        assertEquals(7, bufRowCount(stmt, 0));
        assertEquals(7, bufRowCount(stmt, 1));
    }

    // -----------------------------------------------------------------------
    // null staging
    // -----------------------------------------------------------------------

    @Test
    public void setNull_fixedWidthField_rowAppended() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setNull(1, Types.INTEGER);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setNull_variableWidthField_rowAppended() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setNull(1, Types.VARCHAR);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setString_null_treatedAsNull() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_NCHAR, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setString(1, null);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    // -----------------------------------------------------------------------
    // Unset field defaults to null on flush
    // -----------------------------------------------------------------------

    @Test
    public void unsetField_defaultsToNull_bufferAdvanced() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0)); // not set

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, bufRowCount(stmt, 1)); // null row flushed
    }

    // -----------------------------------------------------------------------
    // Row-count check – equal counts → no exception
    // -----------------------------------------------------------------------

    @Test
    public void rowCountCheck_allEqual_noException() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_BIGINT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        for (int i = 0; i < 3; i++) {
            stmt.setInt(1, i);
            stmt.setLong(2, 100L + i);
            stmt.addBatch();
        }

        // Reflective call to the private consistency check
        java.lang.reflect.Method check =
                WSColumnPreparedStatement.class.getDeclaredMethod("checkRowCounts");
        check.setAccessible(true);
        check.invoke(stmt); // must not throw
    }

    // -----------------------------------------------------------------------
    // Setter variety – fixed-width
    // -----------------------------------------------------------------------

    @Test
    public void setBoolean_staged() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_BOOL, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setBoolean(1, true);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setFloat_staged() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_FLOAT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setFloat(1, 3.14f);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setDouble_staged() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_DOUBLE, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setDouble(1, 2.718281828);
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setBytes_staged() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARBINARY, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setBytes(1, new byte[]{0x01, 0x02, 0x03});
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
    }

    // -----------------------------------------------------------------------
    // tbname staging
    // -----------------------------------------------------------------------

    @Test
    public void tbName_setAndFlushed() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setString(1, "child_table");
        stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        stmt.addBatch();

        assertEquals(0, stmt.getTbNameFieldIdx());
        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, bufRowCount(stmt, 1));
    }

    @Test
    public void tbName_null_throwsSQLException() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        try {
            stmt.setString(1, null);
            fail("Expected SQLException for null tbname");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    public void tbName_notSet_throwsOnFlush() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));

        try {
            stmt.addBatch();
            fail("Expected SQLException for missing tbname");
        } catch (SQLException e) {
            assertTrue("Error must mention table name",
                    e.getMessage().contains("Table name not set"));
        }
    }

    // -----------------------------------------------------------------------
    // clearParameters resets staging state
    // -----------------------------------------------------------------------

    @Test
    public void clearParameters_resetsState() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setInt(1, 42);
        stmt.clearParameters();
        stmt.addBatch(); // null row after clearParameters

        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, stmt.getExpectedRowCount());
    }

    // -----------------------------------------------------------------------
    // Tag fields behave like column fields
    // -----------------------------------------------------------------------

    @Test
    public void tagField_stagedAndFlushed() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TAG.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setInt(1, 99);
        stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        stmt.addBatch();

        assertEquals(1, bufRowCount(stmt, 0));
        assertEquals(1, bufRowCount(stmt, 1));
    }

    // -----------------------------------------------------------------------
    // No-parameter statement
    // -----------------------------------------------------------------------

    @Test
    public void noFields_rowCountStartsAtZero() {
        WSColumnPreparedStatement stmt = buildStmt(new ArrayList<>());
        assertEquals(0, stmt.getExpectedRowCount());
    }

    // -----------------------------------------------------------------------
    // setObject(Types.TIMESTAMP, LocalDateTime) – no recursion, stages correctly
    // -----------------------------------------------------------------------

    @Test
    public void setObject_localDateTimeWithTimestampType_noRecursion_stagesRow() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        LocalDateTime ldt = LocalDateTime.of(2024, 6, 1, 12, 0, 0);

        // Must not throw StackOverflowError (infinite recursion in old code)
        stmt.setObject(1, ldt, Types.TIMESTAMP);
        stmt.addBatch();

        assertEquals(1, stmt.getExpectedRowCount());
        assertEquals(1, bufRowCount(stmt, 0));
    }

    @Test
    public void setObject_localDateTimeWithoutSqlType_stagesRow() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        LocalDateTime ldt = LocalDateTime.of(2024, 6, 1, 12, 0, 0);

        stmt.setObject(1, ldt);
        stmt.addBatch();

        assertEquals(1, stmt.getExpectedRowCount());
        assertEquals(1, bufRowCount(stmt, 0));
    }

    // -----------------------------------------------------------------------
    // clearBatch – drops batched rows, preserves current-row staging
    // -----------------------------------------------------------------------

    @Test
    public void clearBatch_dropsBatchedRows_resetsCountAndBuffers() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);
        stmt.setInt(1, 10);
        stmt.addBatch();
        stmt.setInt(1, 20);
        stmt.addBatch();

        assertEquals(2, stmt.getExpectedRowCount());

        stmt.clearBatch();

        assertEquals("clearBatch must reset row count to 0", 0, stmt.getExpectedRowCount());
        assertEquals("clearBatch must reset column buffer", 0, bufRowCount(stmt, 0));
    }

    @Test
    public void clearBatch_preservesCurrentRowStaging() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_INT, (byte) 0));

        WSColumnPreparedStatement stmt = buildStmt(fields);

        // Batch two rows
        stmt.setInt(1, 10);
        stmt.addBatch();
        stmt.setInt(1, 20);
        stmt.addBatch();

        // Stage a new value (not yet committed to batch)
        stmt.setInt(1, 99);

        // Clear the batch (drops the two committed rows)
        stmt.clearBatch();
        assertEquals(0, stmt.getExpectedRowCount());

        // The staged value (99) must survive clearBatch; flushing it should produce one row
        stmt.addBatch();
        assertEquals("Staged value must survive clearBatch", 1, stmt.getExpectedRowCount());
        assertEquals(1, bufRowCount(stmt, 0));
    }
}
