package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class WsStmtRealPerformanceBenchmarkTest {
    private static final String ENABLE_PROPERTY = "ws.perf.benchmark";
    private static final int TOTAL_ROWS = 1_000_000;
    private static final int ROWS_PER_BATCH = 10_000;
    private static final int BATCH_COUNT = TOTAL_ROWS / ROWS_PER_BATCH;
    private static final String STABLE_NAME = "meters";
    private static final int WARMUP_BATCHES = 2;

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
}
