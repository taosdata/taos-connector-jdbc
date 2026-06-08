package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BLOB;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BOOL;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL128;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_FLOAT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_JSON;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_NCHAR;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UBIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARBINARY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbsWSPreparedStatementSetObjectTest {

    @Test
    public void setObjectWithTargetSqlType_convertsSupportedValues() throws Exception {
        TSWSPreparedStatement stmt = newStmt(ZoneId.of("UTC"));
        int index = 1;

        stmt.setObject(index, null, Types.INTEGER);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_INT, null);

        stmt.setObject(index, true, Types.BOOLEAN);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BOOL, true);
        stmt.setObject(index, 0, Types.BOOLEAN);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BOOL, false);

        stmt.setObject(index, false, Types.TINYINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_TINYINT, (byte) 0);
        stmt.setObject(index, 7, Types.TINYINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_TINYINT, (byte) 7);

        stmt.setObject(index, true, Types.SMALLINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_SMALLINT, (short) 1);
        stmt.setObject(index, 8, Types.SMALLINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_SMALLINT, (short) 8);

        stmt.setObject(index, false, Types.INTEGER);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_INT, 0);
        stmt.setObject(index, 9L, Types.INTEGER);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_INT, 9);

        stmt.setObject(index, true, Types.BIGINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BIGINT, 1L);
        stmt.setObject(index, 10, Types.BIGINT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BIGINT, 10L);

        stmt.setObject(index, true, Types.FLOAT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_FLOAT, 1.0f);
        stmt.setObject(index, 1.5d, Types.FLOAT);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_FLOAT, 1.5f);

        stmt.setObject(index, false, Types.DOUBLE);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_DOUBLE, 0.0d);
        stmt.setObject(index, 2.5f, Types.DOUBLE);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_DOUBLE, 2.5d);

        Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(1000L));
        stmt.setObject(index, timestamp, Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, new java.sql.Date(2000L), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, new Time(3000L), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, LocalDateTime.of(2024, 1, 2, 3, 4, 5), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, Instant.ofEpochMilli(4000L), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, ZonedDateTime.ofInstant(Instant.ofEpochMilli(5000L), ZoneId.of("UTC")), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);
        stmt.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(6000L), ZoneId.of("UTC")), Types.TIMESTAMP);
        assertColumnType(stmt, index++, TSDB_DATA_TYPE_TIMESTAMP);

        stmt.setObject(index, "binary", Types.VARCHAR);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BINARY, bytes("binary"));
        stmt.setObject(index, bytes("bin"), Types.BINARY);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BINARY, bytes("bin"));

        stmt.setObject(index, bytes("blob"), Types.BLOB);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BLOB, bytes("blob"));
        stmt.setObject(index, new SerialBlob(bytes("blob2")), Types.BLOB);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_BLOB, bytes("blob2"));

        stmt.setObject(index, bytes("varbin"), Types.VARBINARY);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_VARBINARY, bytes("varbin"));
        stmt.setObject(index, "varbin2", Types.VARBINARY);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_VARBINARY, bytes("varbin2"));

        stmt.setObject(index, bytes("nchar"), Types.NCHAR);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_NCHAR, bytes("nchar"));
        stmt.setObject(index, "nchar2", Types.NCHAR);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_NCHAR, bytes("nchar2"));

        stmt.setObject(index, 11L, Types.OTHER);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_UBIGINT, 11L);
        stmt.setObject(index, "{\"k\":1}", Types.OTHER);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_JSON, "{\"k\":1}");

        stmt.setObject(index, new BigDecimal("12.30"), Types.DECIMAL);
        assertColumn(stmt, index++, TSDB_DATA_TYPE_DECIMAL128, bytes("12.30"));

        stmt.setNull(index, Types.VARCHAR, "ignored");
        assertColumn(stmt, index, TSDB_DATA_TYPE_BINARY, null);
    }

    @Test
    public void setObjectWithTargetSqlType_convertsLocalDateTimeWithoutZone() throws Exception {
        TSWSPreparedStatement stmt = newStmt(null);

        stmt.setObject(1, LocalDateTime.of(2024, 1, 2, 3, 4, 5), Types.TIMESTAMP);

        assertColumnType(stmt, 1, TSDB_DATA_TYPE_TIMESTAMP);
    }

    @Test
    public void setObjectWithTargetSqlType_rejectsUnsupportedValues() throws Exception {
        TSWSPreparedStatement stmt = newStmt(ZoneId.of("UTC"));

        assertSqlException(() -> stmt.setObject(1, "true", Types.BOOLEAN));
        assertSqlException(() -> stmt.setObject(2, "tiny", Types.TINYINT));
        assertSqlException(() -> stmt.setObject(3, "small", Types.SMALLINT));
        assertSqlException(() -> stmt.setObject(4, "int", Types.INTEGER));
        assertSqlException(() -> stmt.setObject(5, "long", Types.BIGINT));
        assertSqlException(() -> stmt.setObject(6, "float", Types.FLOAT));
        assertSqlException(() -> stmt.setObject(7, "double", Types.DOUBLE));
        assertSqlException(() -> stmt.setObject(8, "time", Types.TIMESTAMP));
        assertSqlException(() -> stmt.setObject(9, 1, Types.BINARY));
        assertSqlException(() -> stmt.setObject(10, 1, Types.BLOB));
        assertSqlException(() -> stmt.setObject(11, 1, Types.VARBINARY));
        assertSqlException(() -> stmt.setObject(12, 1, Types.NCHAR));
        assertSqlException(() -> stmt.setObject(13, 1, Types.DECIMAL));
        assertSqlException(() -> stmt.setObject(14, 1, Types.ARRAY));
    }

    private static TSWSPreparedStatement newStmt(ZoneId zoneId) throws SQLException {
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);

        ConnectionParam param = mock(ConnectionParam.class);
        when(param.getZoneId()).thenReturn(zoneId);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(transport.getConnectionParam()).thenReturn(param);

        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setStmtId(1L);
        return new TSWSPreparedStatement(
                transport,
                param,
                "test_db",
                mock(AbstractConnection.class),
                "INSERT INTO t VALUES (?)",
                1L,
                prepareResp);
    }

    private static void assertColumn(TSWSPreparedStatement stmt, int index, int type, Object expected) {
        Column column = stmt.colOrderedMap.get(index);
        assertEquals(type, column.getType());
        if (expected instanceof byte[]) {
            assertArrayEquals((byte[]) expected, (byte[]) column.getData());
        } else {
            assertEquals(expected, column.getData());
        }
    }

    private static void assertColumnType(TSWSPreparedStatement stmt, int index, int type) {
        Column column = stmt.colOrderedMap.get(index);
        assertEquals(type, column.getType());
        assertTrue(column.getData() instanceof Instant);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertSqlException(ThrowingRunnable runnable) throws Exception {
        try {
            runnable.run();
            fail("Expected SQLException");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage() != null && !expected.getMessage().isEmpty());
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
