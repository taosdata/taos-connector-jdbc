package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for the column-data API path in {@link AbsWSPreparedStatement}.
 *
 * <p>Task 4: verifies that the existing column-data API (setTableName / tag setters /
 * list-based column setters / columnDataAddBatch / columnDataExecuteBatch) correctly
 * produces {@link Stmt2ColumnFieldBuffer}s via the shared serializer building blocks.
 *
 * <p>Tests run without a live server; Transport and WSConnection are stubbed via Mockito.
 * The {@link TSWSPreparedStatement} subclass is used as the concrete statement so that
 * the package-private test accessors on {@link AbsWSPreparedStatement} are reachable.
 */
public class AbsWSPreparedStatementColumnDataTest {

    private Transport transport;
    private ConnectionParam param;

    // -----------------------------------------------------------------------
    // Setup
    // -----------------------------------------------------------------------

    @Before
    public void setUp() {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);

        Mockito.when(transport.getConnectionParam()).thenReturn(param);
        Mockito.when(transport.getReconnectCount()).thenReturn(0);
        Mockito.when(transport.isConnected()).thenReturn(true);
        Mockito.when(transport.isClosed()).thenReturn(false);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        Mockito.when(param.getZoneId()).thenReturn(null);
        Mockito.when(param.getRetryTimes()).thenReturn(1);
    }

    // -----------------------------------------------------------------------
    // Factory helpers
    // -----------------------------------------------------------------------

    /** Creates a Field descriptor with the given bind/field types. */
    private static Field makeField(byte bindType, byte fieldType) {
        Field f = new Field();
        f.setBindType(bindType);
        f.setFieldType(fieldType);
        f.setPrecision((byte) 0); // ms precision
        return f;
    }

    /** Builds a Stmt2PrepareResp for an insert with the supplied field list. */
    private static Stmt2PrepareResp makeInsertResp(List<Field> fields) {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(true);
        resp.setStmtId(1L);
        resp.setFields(fields);
        return resp;
    }

    /**
     * Creates a TSWSPreparedStatement backed by a WSConnection whose server version
     * is set so that {@link WSConnection#supportsStmt2BindExec()} returns the specified value.
     */
    private TSWSPreparedStatement buildStmt(List<Field> fields, boolean bindExecServer) throws Exception {
        AbstractConnection connection;
        if (bindExecServer) {
            // Use WSConnection with a version that supports bind-exec (>= 3.1.4.10)
            java.util.Properties props = new java.util.Properties();
            WSConnection wsConn = new WSConnection(
                    "jdbc:TAOS-RS://localhost:6041/testdb",
                    props, transport, param, "3.1.4.10");
            connection = wsConn;
        } else {
            // Use a plain AbstractConnection mock (non-WSConnection → no bind-exec)
            connection = Mockito.mock(AbstractConnection.class);
        }

        Stmt2PrepareResp resp = makeInsertResp(fields);
        return new TSWSPreparedStatement(transport, param, "testdb", connection,
                "INSERT INTO t USING s TAGS(?) VALUES(?,?)", 1L, resp);
    }

    // -----------------------------------------------------------------------
    // Bind-exec routing: version gating
    // -----------------------------------------------------------------------

    @Test
    public void bindExecEnabled_whenServerSupportsIt() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, /* bindExecServer= */ true);
        assertTrue("bind-exec should be enabled on a bind-exec capable server",
                stmt.isUsingBindExecForTesting());
    }

    @Test
    public void bindExecDisabled_whenServerDoesNotSupportIt() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, /* bindExecServer= */ false);
        assertFalse("bind-exec should be disabled when connection is not a WSConnection",
                stmt.isUsingBindExecForTesting());
    }

    // -----------------------------------------------------------------------
    // buildColumnBuffersFromTableInfoMap – normal table (no TBNAME, no TAG)
    // -----------------------------------------------------------------------

    @Test
    public void normalTable_singleColumn_oneRow() throws Exception {
        // Schema: one COL of type INT
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setInt(0, Arrays.asList(42));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals("field count should equal schema size", 1, bufs.length);
        assertEquals("INT column should have 1 row", 1, bufs[0].getRowCount());
    }

    @Test
    public void normalTable_singleColumn_multipleRows() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BIGINT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setLong(0, Arrays.asList(1L, 2L, 3L, 4L, 5L));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(1, bufs.length);
        assertEquals("5 rows should be recorded", 5, bufs[0].getRowCount());
    }

    @Test
    public void normalTable_multipleColumns_rowCountConsistent() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_DOUBLE));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setTimestamp(0, Arrays.asList(1000L, 2000L, 3000L));
        stmt.setInt(1, Arrays.asList(10, 20, 30));
        stmt.setDouble(2, Arrays.asList(1.1, 2.2, 3.3));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(3, bufs.length);
        for (int i = 0; i < 3; i++) {
            assertEquals("every field buffer must have 3 rows", 3, bufs[i].getRowCount());
        }
    }

    // -----------------------------------------------------------------------
    // buildColumnBuffersFromTableInfoMap – null handling
    // -----------------------------------------------------------------------

    @Test
    public void nullValues_areRecordedCorrectly() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BINARY));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        // Mix of non-null and null
        stmt.setInt(0, Arrays.asList(1, null, 3));
        stmt.setString(1, Arrays.asList("hello", null, "world"), 16);
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(3, bufs[0].getRowCount());  // INT column: 3 rows
        assertEquals(3, bufs[1].getRowCount());  // BINARY column: 3 rows
    }

    // -----------------------------------------------------------------------
    // buildColumnBuffersFromTableInfoMap – supertable (TBNAME + TAG + COL)
    // -----------------------------------------------------------------------

    @Test
    public void supertableInsert_tbNameRepeatedPerRow() throws Exception {
        // Schema: TBNAME + 1 TAG (INT) + 1 COL (BIGINT)
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), (byte) TSDB_DATA_TYPE_INT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BIGINT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        // Table "t1" with 3 rows
        stmt.setTableName("t1");
        stmt.setTagInt(0, 99);
        stmt.setLong(0, Arrays.asList(10L, 20L, 30L));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(3, bufs.length);
        assertEquals("TBNAME buffer should have 3 rows (one per data row)", 3, bufs[0].getRowCount());
        assertEquals("TAG buffer should have 3 rows", 3, bufs[1].getRowCount());
        assertEquals("COL buffer should have 3 rows", 3, bufs[2].getRowCount());
    }

    @Test
    public void supertableInsert_multipleTablesAggregated() throws Exception {
        // Schema: TBNAME + 1 COL (INT)
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        // Table "t1" → 2 rows
        stmt.setTableName("t1");
        stmt.setInt(0, Arrays.asList(1, 2));
        stmt.columnDataAddBatch();

        // Table "t2" → 3 rows
        stmt.setTableName("t2");
        stmt.setInt(0, Arrays.asList(10, 20, 30));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        // Total rows = 2 + 3 = 5
        assertEquals("TBNAME buffer: t1(×2) + t2(×3) = 5 rows", 5, bufs[0].getRowCount());
        assertEquals("COL buffer: 2 + 3 = 5 rows", 5, bufs[1].getRowCount());

        // Table count derived from sequential runs in TBNAME buffer
        assertEquals("two distinct tables → tableCount should be 2", 2, bufs[0].computeTableCount());
    }

    // -----------------------------------------------------------------------
    // Type coverage for appendObjectToBuffer (via column-data list setters)
    // -----------------------------------------------------------------------

    @Test
    public void allFixedWidthTypes_areConvertedWithoutError() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BOOL));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_TINYINT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_SMALLINT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BIGINT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_FLOAT));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_DOUBLE));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setBoolean(0, Arrays.asList(true, false));
        stmt.setByte(1, Arrays.asList((byte) 1, (byte) 2));
        stmt.setShort(2, Arrays.asList((short) 100, (short) 200));
        stmt.setInt(3, Arrays.asList(1000, 2000));
        stmt.setLong(4, Arrays.asList(1_000_000L, 2_000_000L));
        stmt.setFloat(5, Arrays.asList(1.5f, 2.5f));
        stmt.setDouble(6, Arrays.asList(1.234, 5.678));
        stmt.setTimestamp(7, Arrays.asList(1_700_000_000_000L, 1_700_000_001_000L));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(8, bufs.length);
        for (int i = 0; i < 8; i++) {
            assertEquals("each buffer should have 2 rows", 2, bufs[i].getRowCount());
        }
    }

    @Test
    public void variableWidthTypes_areConvertedWithoutError() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_NCHAR));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setString(0, Arrays.asList("hello", "world"), 16);
        stmt.setNString(1, Arrays.asList("foo", "bar"), 16);
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(2, bufs.length);
        assertEquals(2, bufs[0].getRowCount());
        assertEquals(2, bufs[1].getRowCount());
    }

    @Test
    public void tagTimestamp_storedAsInstant_convertedCorrectly() throws Exception {
        // setTagTimestamp(Timestamp) stores an Instant; must survive appendObjectToBuffer
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setTableName("t_ts_tag");
        stmt.setTagTimestamp(0, new Timestamp(1_700_000_000_000L));
        stmt.setInt(0, Arrays.asList(1, 2));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(2, bufs[1].getRowCount()); // TAG repeated 2 times
    }

    @Test
    public void tagTimestamp_storedAsLong_convertedCorrectly() throws Exception {
        // setTagTimestamp(long) stores a Timestamp object (not Instant); must survive
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setTableName("t_ts_tag2");
        stmt.setTagTimestamp(0, 1_700_000_000_000L);
        stmt.setInt(0, Arrays.asList(10));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals("TAG buffer should have 1 row", 1, bufs[1].getRowCount());
    }

    @Test
    public void bigIntegerUBigInt_convertedCorrectly() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_UBIGINT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setBigInteger(0, Arrays.asList(BigInteger.valueOf(18_000_000_000_000_000L)));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        assertEquals(1, bufs[0].getRowCount());
    }

    // -----------------------------------------------------------------------
    // Public column-data API contract preserved
    // -----------------------------------------------------------------------

    @Test
    public void publicApi_setTableName_preservedOnLegacyServer() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_BINARY));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        // Legacy server: non-WSConnection mock
        TSWSPreparedStatement stmt = buildStmt(fields, false);

        assertFalse("legacy server should not use bind-exec", stmt.isUsingBindExecForTesting());

        // setTableName should not throw
        stmt.setTableName("my_table");
    }

    @Test
    public void columnDataAddBatch_clearsInternalQueues() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setInt(0, Arrays.asList(1, 2, 3));
        stmt.columnDataAddBatch();

        // After addBatch, tag queue and colListQueue should be drained;
        // calling buildColumnBuffersForTesting should succeed with accumulated data
        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();
        assertEquals(3, bufs[0].getRowCount());
    }

    @Test
    public void emptyFields_throwsOnBindExecConversion() throws Exception {
        // Build a stmt where fields is empty to ensure graceful error
        List<Field> fields = new ArrayList<>();
        // No fields → buildColumnBuffersForTesting should throw
        TSWSPreparedStatement stmt = buildStmt(fields, false);

        try {
            stmt.buildColumnBuffersForTesting();
            fail("Expected SQLException when field metadata is empty");
        } catch (SQLException e) {
            assertTrue("error message should mention field metadata",
                    e.getMessage().contains("field metadata"));
        }
    }

    // -----------------------------------------------------------------------
    // Serializer output is valid
    // -----------------------------------------------------------------------

    @Test
    public void serializerOutput_hasCorrectFieldCount() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP));
        fields.add(makeField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT));

        TSWSPreparedStatement stmt = buildStmt(fields, false);

        stmt.setTimestamp(0, Arrays.asList(1000L, 2000L));
        stmt.setInt(1, Arrays.asList(10, 20));
        stmt.columnDataAddBatch();

        Stmt2ColumnFieldBuffer[] bufs = stmt.buildColumnBuffersForTesting();

        // Use Stmt2ColumnBindSerializer to produce the payload and verify header
        byte[] payload = com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.serialize(bufs);

        // Header layout: total_length(4) | row_count(4) | table_count(4) | field_count(4) | field_offset(4)
        int rowCount = readLE32(payload, 4);
        int tableCount = readLE32(payload, 8);
        int fieldCount = readLE32(payload, 12);

        assertEquals("row_count should be 2", 2, rowCount);
        assertEquals("table_count should be 1 (single table)", 1, tableCount);
        assertEquals("field_count should be 2", 2, fieldCount);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Reads a little-endian 32-bit int from buf[offset..offset+3]. */
    private static int readLE32(byte[] buf, int offset) {
        return (buf[offset] & 0xFF)
                | ((buf[offset + 1] & 0xFF) << 8)
                | ((buf[offset + 2] & 0xFF) << 16)
                | ((buf[offset + 3] & 0xFF) << 24);
    }
}
