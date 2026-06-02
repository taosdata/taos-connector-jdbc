package com.taosdata.jdbc.ws.manual;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSColumnFastPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import org.junit.Assume;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Manual end-to-end websocket benchmark.
 *
 * <p>Run with:
 * <pre>
 * mvn test -Dtest=WsStmtRealPerformanceBenchmarkTest -Dws.perf.benchmark=true -Djacoco.skip=true
 * </pre>
 *
 * <p>Compares {@link WSColumnFastPreparedStatement} through both the default fast route and the
 * {@code stmt2BindMode=jdbc} alias, plus {@link WSRowPreparedStatement} using 100 batches x 10,000 rows
 * with one row per subtable.
 */
public class WsStmtRealPerformanceBenchmarkTest {
    private static final String DB_NAME = TestUtils.camelToSnake(WsStmtRealPerformanceBenchmarkTest.class);
    private static final int TOTAL_ROWS = 2_000_000;
    private static final int SUB_TABLE_NUM = 10_000;

    private static final int ROWS_PER_BATCH = 10_000;
    private static final int BATCH_COUNT = TOTAL_ROWS / ROWS_PER_BATCH;
    private static final String STABLE_NAME = "meters";
    private static final int BENCHMARK_MESSAGE_WAIT_TIMEOUT_MS = 600_000;
    private static final long BASE_TS = 1_700_000_000_000L;
    private static final String INSERT_SQL =
            "insert into " +  STABLE_NAME + " (tbname, ts, current, voltage, phase) values(?, ?, ?, ?, ?)";

    private enum RouteMode {
        FAST,
        JDBC,
        LINE
    }

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
    public void benchmark_stmt2BindExecVsJdbcAndLineMode_autoCreateSubtables() throws Exception {
        Assume.assumeTrue(
                "Manual benchmark disabled; run with -Dws.perf.benchmark=true",
                Boolean.getBoolean("ws.perf.benchmark"));
        assumeStmt2BindExecSupported();

        BenchmarkResult line = runPath(RouteMode.LINE, "line_mode");
        BenchmarkResult jdbc = runPath(RouteMode.JDBC, "jdbc_fast_alias");
        BenchmarkResult fast = runPath(RouteMode.FAST, "fast_stmt2");

        printSummary(line, jdbc, fast);

        assertEquals(TOTAL_ROWS, line.insertedRows);
        assertEquals(TOTAL_ROWS, jdbc.insertedRows);
        assertEquals(TOTAL_ROWS, fast.insertedRows);
        assertTrue(line.elapsedNs > 0L);
        assertTrue(jdbc.elapsedNs > 0L);
        assertTrue(fast.elapsedNs > 0L);
    }

    private static String buildWsUrl(RouteMode routeMode, String database) {
        String base = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (base == null) {
            base = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort() + "/";
        }
        StringBuilder url = new StringBuilder(base);
        if (database != null && !database.isEmpty()) {
            if (url.charAt(url.length() - 1) != '/') {
                url.append('/');
            }
            url.append(database);
        }
        url
                .append("?user=").append(TestEnvUtil.getUser())
                .append("&password=").append(TestEnvUtil.getPassword());
        return url.toString();
    }

    private static String tableNameFor(long rowIndex) {
        return String.format("d%09d", rowIndex % SUB_TABLE_NUM);
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

    private static void recreateSchema(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database " + dbName);
            stmt.execute("use " + dbName);
            stmt.execute("create stable " + STABLE_NAME + " (" +
                    "ts timestamp, current float, voltage int, phase float) " +
                    "tags(location varchar(64), group_id int)");

            StringBuilder sql = new StringBuilder();
            sql.append("create table");
            for (int tableNum = 0; tableNum < 10000; tableNum++) {
                StringBuilder subSql = new StringBuilder();
                subSql.append(" if not exists ")
                        .append(tableNameFor(tableNum))
                        .append(" using ")
                        .append(STABLE_NAME)
                        .append(" tags(")
                        .append("\"location" + tableNum + " \"")
                        .append(",")
                        .append(tableNum)
                        .append(")");

                sql.append(subSql);

                if (tableNum % 100 == 0) {
                    stmt.execute(sql.toString());
                    sql = new StringBuilder();
                    sql.append("create table");
                }
            }
            if (sql.length() > "create table".length()) {
                stmt.execute(sql.toString());
            }
        }
    }

    private static void dropDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
        }
    }

    private static Connection openConnection(RouteMode routeMode) throws SQLException {
        return openConnection(routeMode, null);
    }

    private static Connection openConnection(RouteMode routeMode, String database) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "false");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "false");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RETRY_TIMES, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT,
                String.valueOf(BENCHMARK_MESSAGE_WAIT_TIMEOUT_MS));
        return DriverManager.getConnection(buildWsUrl(routeMode, database), properties);
    }

    private static void assumeStmt2BindExecSupported() throws SQLException {
        try (Connection conn = openConnection(RouteMode.FAST)) {
            Assume.assumeTrue("Benchmark requires WSConnection", conn instanceof WSConnection);
            WSConnection wsConn = (WSConnection) conn;
            Assume.assumeTrue(
                    "Benchmark requires a stmt2_bind_exec capable server; current server routes default inserts through legacy mode",
                    wsConn.supportsStmt2BindExec());
        }
    }

    private BenchmarkResult runPath(RouteMode routeMode, String label) throws Exception {
        try (Connection adminConn = openConnection(routeMode)) {
            recreateSchema(adminConn, DB_NAME);
        }
        try (Connection conn = openConnection(routeMode, DB_NAME)) {
            try {
                long insertedRows = 0L;
                long totalBatchNs = 0L;

                try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {

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
                    verifyCounts(conn);
                    return new BenchmarkResult(label, elapsedNs, insertedRows, totalBatchNs / BATCH_COUNT);
                }
            } finally {
                cleanupDatabase(conn);
            }
        }
    }

    private static void bindBatch(PreparedStatement pstmt, long startRow, int rows) throws SQLException {
        for (int row = 0; row < rows; row++) {
            bindRow(pstmt, startRow + row, tableNameFor(startRow + row));
        }
    }

    private static void bindRow(
            PreparedStatement pstmt,
            long rowIndex,
            String tableName) throws SQLException {
        pstmt.setString(1, tableName);
        pstmt.setTimestamp(2, timestampFor(rowIndex));
        pstmt.setFloat(3, currentFor(rowIndex));
        pstmt.setInt(4, voltageFor(rowIndex));
        pstmt.setFloat(5, phaseFor(rowIndex));
        pstmt.addBatch();
    }

    private static long queryCount(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long queryStableCount(Connection conn) throws SQLException {
        return queryCount(conn, "select count(*) from " + STABLE_NAME);
    }

    private static long countSubTables(Connection conn) throws SQLException {
        long tableCount = 0L;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("show tables like 'd%'")) {
            while (rs.next()) {
                tableCount++;
            }
        }
        return tableCount;
    }

    private static void verifyCounts(Connection conn) throws SQLException {
        long rows = queryStableCount(conn);
        long tableCount = countSubTables(conn);
        assertEquals(TOTAL_ROWS, rows);
        assertEquals(SUB_TABLE_NUM, tableCount);
    }

    private static long rowsPerSecond(BenchmarkResult result) {
        return (result.insertedRows * 1_000_000_000L) / result.elapsedNs;
    }

    private static void printSummary(BenchmarkResult line, BenchmarkResult jdbc, BenchmarkResult fast) {
        double jdbcVsLine = (double) line.elapsedNs / (double) jdbc.elapsedNs;
        double fastVsLine = (double) line.elapsedNs / (double) fast.elapsedNs;
        printResult(line);
        printResult(jdbc);
        printResult(fast);
        System.out.printf("[WsStmtPerf] line_baseline jdbc_vs_line=%.3f fast_vs_line=%.3f%n",
                jdbcVsLine,
                fastVsLine);
    }

    private static void printResult(BenchmarkResult result) {
        System.out.printf("[WsStmtPerf] %s total=%d elapsedMs=%.2f rowsPerSec=%d avgBatchMs=%.2f%n",
                result.label,
                result.insertedRows,
                result.elapsedNs / 1_000_000.0,
                rowsPerSecond(result),
                result.averageBatchNs / 1_000_000.0);
    }

    private static void cleanupDatabase(Connection conn) {
//        try {
//            dropDatabase(conn);
//        } catch (SQLException e) {
//            System.err.println("Failed to drop benchmark database " + DB_NAME + ": " + e.getMessage());
//        }
    }
}
