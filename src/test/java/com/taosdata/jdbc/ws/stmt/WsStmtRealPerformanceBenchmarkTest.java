package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.ws.WSColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSRowPreparedStatement;
import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Manual end-to-end websocket benchmark.
 *
 * <p>Run with:
 * <pre>
 * mvn test -Dtest=WsStmtRealPerformanceBenchmarkTest -Dws.perf.benchmark=true -Djacoco.skip=true
 * </pre>
 *
 * <p>Compares {@link WSColumnPreparedStatement} and {@link WSRowPreparedStatement}
 * using 100 batches x 10,000 rows with one row per subtable.
 */
public class WsStmtRealPerformanceBenchmarkTest {
    private static final String ENABLE_PROPERTY = "ws.perf.benchmark";
    private static final int TOTAL_ROWS = 1_000_000;
    private static final int ROWS_PER_BATCH = 10_000;
    private static final int BATCH_COUNT = TOTAL_ROWS / ROWS_PER_BATCH;
    private static final String STABLE_NAME = "meters";
    private static final int WARMUP_BATCHES = 2;
    private static final int WARMUP_ROWS_PER_BATCH = 100;
    private static final long BASE_TS = 1_700_000_000_000L;
    private static final String INSERT_SQL =
            "insert into ? using " + STABLE_NAME + " tags(?, ?) values(?, ?, ?, ?)";
    private static final String DB_PREFIX = "ws_stmt_perf_bench";

    private static final class BenchmarkResult {
        final String label;
        final long elapsedNs;
        final long insertedRows;
        final long averageBatchNs;

        BenchmarkResult(String label, long elapsedNs, long insertedRows, long averageBatchNs) {
            this.label = label;
            this.elapsedNs = elapsedNs;
            this.insertedRows = insertedRows;
            this.averageBatchNs = averageBatchNs;
        }
    }

    @Test
    public void benchmark_stmt2BindExecVsLineMode_autoCreateSubtables() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY));

        long runId = System.currentTimeMillis();
        BenchmarkResult columnar = runPath(false, "columnar", runId);
        BenchmarkResult row = runPath(true, "line_mode", runId);

        printSummary(columnar, row);

        assertEquals(TOTAL_ROWS, columnar.insertedRows);
        assertEquals(TOTAL_ROWS, row.insertedRows);
        assertTrue(columnar.elapsedNs > 0L);
        assertTrue(row.elapsedNs > 0L);
    }

    @Test
    public void columnarUrl_defaultWsConnection_doesNotInjectLineMode() {
        String url = buildWsUrl(false);
        assertTrue(url.startsWith("jdbc:TAOS-WS://"));
        assertFalse(url.contains("pbsMode=line"));
    }

    @Test
    public void rowUrl_lineModeConnection_injectsLineMode() {
        String url = buildWsUrl(true);
        assertTrue(url.contains("pbsMode=line"));
    }

    @Test
    public void tableName_isDeterministicAndUnique() {
        assertEquals("d000000000", tableNameFor(0));
        assertEquals("d000000123", tableNameFor(123));
        assertNotEquals(tableNameFor(123), tableNameFor(124));
    }

    @Test
    public void rowsPerSecond_isDerivedFromMeasuredRowsAndElapsedTime() {
        BenchmarkResult result = new BenchmarkResult("columnar", 2_000_000_000L, 1_000_000L, 10_000_000L);
        assertEquals(500_000L, rowsPerSecond(result));
    }

    private static String buildWsUrl(boolean lineMode) {
        String base = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (base == null) {
            base = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort() + "/";
        }
        StringBuilder url = new StringBuilder(base)
                .append("?user=").append(TestEnvUtil.getUser())
                .append("&password=").append(TestEnvUtil.getPassword());
        if (lineMode) {
            url.append("&pbsMode=line");
        }
        return url.toString();
    }

    private static String tableNameFor(long rowIndex) {
        return String.format("d%09d", rowIndex);
    }

    private static String locationFor(long rowIndex) {
        return "loc-" + (rowIndex % 1000);
    }

    private static int groupIdFor(long rowIndex) {
        return (int) (rowIndex % 100);
    }

    private static float currentFor(long rowIndex) {
        return (float) (rowIndex % 1_000) / 10.0f;
    }

    private static int voltageFor(long rowIndex) {
        return 220 + (int) (rowIndex % 50);
    }

    private static float phaseFor(long rowIndex) {
        return (float) (rowIndex % 360) / 10.0f;
    }

    private static Timestamp timestampFor(long rowIndex) {
        return new Timestamp(BASE_TS + rowIndex);
    }

    private static String buildDbName(String label, long runId) {
        return DB_PREFIX + "_" + label + "_" + runId;
    }

    private static void recreateSchema(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database " + dbName);
            stmt.execute("use " + dbName);
            stmt.execute("create stable " + STABLE_NAME + " (" +
                    "ts timestamp, current float, voltage int, phase float) " +
                    "tags(location varchar(64), group_id int)");
        }
    }

    private static void dropDatabase(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
        }
    }

    private static Connection openConnection(boolean lineMode) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "false");
        return DriverManager.getConnection(buildWsUrl(lineMode), properties);
    }

    private BenchmarkResult runPath(boolean lineMode, String label, long runId) throws Exception {
        String dbName = buildDbName(label, runId);
        try (Connection conn = openConnection(lineMode)) {
            recreateSchema(conn, dbName);
            try {
                warmup(lineMode, label, runId);
                long insertedRows = 0L;
                long totalBatchNs = 0L;

                try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                    assertRoute(pstmt, lineMode);

                    long started = System.nanoTime();
                    for (int batch = 0; batch < BATCH_COUNT; batch++) {
                        long batchStart = System.nanoTime();
                        long batchBaseRow = (long) batch * ROWS_PER_BATCH;
                        bindBatch(pstmt, batchBaseRow, ROWS_PER_BATCH);
                        int[] results = pstmt.executeBatch();
                        assertEquals(ROWS_PER_BATCH, results.length);
                        insertedRows += ROWS_PER_BATCH;
                        totalBatchNs += System.nanoTime() - batchStart;
                    }
                    long elapsedNs = System.nanoTime() - started;
                    verifyCounts(conn, dbName);
                    return new BenchmarkResult(label, elapsedNs, insertedRows, totalBatchNs / BATCH_COUNT);
                }
            } finally {
                cleanupDatabase(conn, dbName);
            }
        }
    }

    private void warmup(boolean lineMode, String label, long runId) throws Exception {
        String warmupDb = buildDbName(label + "_warmup", runId);
        try (Connection conn = openConnection(lineMode)) {
            recreateSchema(conn, warmupDb);
            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                assertRoute(pstmt, lineMode);
                for (int batch = 0; batch < WARMUP_BATCHES; batch++) {
                    long batchBaseRow = (long) batch * WARMUP_ROWS_PER_BATCH;
                    bindWarmupBatch(pstmt, batchBaseRow, WARMUP_ROWS_PER_BATCH);
                    pstmt.executeBatch();
                }
            } finally {
                cleanupDatabase(conn, warmupDb);
            }
        }
    }

    private static void assertRoute(PreparedStatement pstmt, boolean lineMode) {
        if (lineMode) {
            assertTrue("Expected WSRowPreparedStatement, got " + pstmt.getClass().getName(),
                    pstmt instanceof WSRowPreparedStatement);
        } else {
            assertTrue("Expected WSColumnPreparedStatement, got " + pstmt.getClass().getName(),
                    pstmt instanceof WSColumnPreparedStatement);
        }
    }

    private static void bindBatch(PreparedStatement pstmt, long startRow, int rows) throws SQLException {
        for (int row = 0; row < rows; row++) {
            bindRow(pstmt, startRow + row, tableNameFor(startRow + row), locationFor(startRow + row), groupIdFor(startRow + row));
        }
    }

    private static void bindWarmupBatch(PreparedStatement pstmt, long startRow, int rows) throws SQLException {
        for (int row = 0; row < rows; row++) {
            long globalRow = startRow + row;
            bindRow(pstmt, globalRow, "warmup_" + globalRow, "warmup", (int) globalRow);
        }
    }

    private static void bindRow(
            PreparedStatement pstmt,
            long rowIndex,
            String tableName,
            String location,
            int groupId) throws SQLException {
        pstmt.setString(1, tableName);
        pstmt.setString(2, location);
        pstmt.setInt(3, groupId);
        pstmt.setTimestamp(4, timestampFor(rowIndex));
        pstmt.setFloat(5, currentFor(rowIndex));
        pstmt.setInt(6, voltageFor(rowIndex));
        pstmt.setFloat(7, phaseFor(rowIndex));
        pstmt.addBatch();
    }

    private static long queryCount(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long queryStableCount(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("use " + dbName);
        }
        return queryCount(conn, "select count(*) from " + STABLE_NAME);
    }

    private static long countSubTables(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("use " + dbName);
        }
        long tableCount = 0L;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("show tables like 'd%'")) {
            while (rs.next()) {
                tableCount++;
            }
        }
        return tableCount;
    }

    private static void verifyCounts(Connection conn, String dbName) throws SQLException {
        long stableCount = queryStableCount(conn, dbName);
        long tableCount = countSubTables(conn, dbName);
        assertEquals(TOTAL_ROWS, stableCount);
        assertEquals(TOTAL_ROWS, tableCount);
    }

    private static long rowsPerSecond(BenchmarkResult result) {
        return (result.insertedRows * 1_000_000_000L) / result.elapsedNs;
    }

    private static void printSummary(BenchmarkResult columnar, BenchmarkResult row) {
        double speedup = (double) row.elapsedNs / (double) columnar.elapsedNs;
        System.out.printf("[WsStmtPerf] %s total=%d elapsedMs=%.2f rowsPerSec=%d avgBatchMs=%.2f%n",
                columnar.label,
                columnar.insertedRows,
                columnar.elapsedNs / 1_000_000.0,
                rowsPerSecond(columnar),
                columnar.averageBatchNs / 1_000_000.0);
        System.out.printf("[WsStmtPerf] %s total=%d elapsedMs=%.2f rowsPerSec=%d avgBatchMs=%.2f%n",
                row.label,
                row.insertedRows,
                row.elapsedNs / 1_000_000.0,
                rowsPerSecond(row),
                row.averageBatchNs / 1_000_000.0);
        System.out.printf("[WsStmtPerf] speedup columnar_vs_row=%.3f%n", speedup);
    }

    private static void cleanupDatabase(Connection conn, String dbName) {
        try {
            dropDatabase(conn, dbName);
        } catch (SQLException e) {
            System.err.println("Failed to drop benchmark database " + dbName + ": " + e.getMessage());
        }
    }
}
