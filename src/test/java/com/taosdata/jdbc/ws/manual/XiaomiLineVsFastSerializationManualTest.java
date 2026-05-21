package com.taosdata.jdbc.ws.manual;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.WSColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSColumnFastPreparedStatement;
import com.taosdata.jdbc.ws.WSRowPreparedStatement;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BOOL;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL128;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL64;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_FLOAT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_GEOMETRY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_NCHAR;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UBIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_USMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UTINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARBINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XiaomiLineVsFastSerializationManualTest {
    private static final long BASE_TS = 1_700_000_000_000L;
    private static final int EXTENSION_MIN_BYTES = 400;
    private static final String XIAOMI_DDL_RESOURCE = "/manual/xiaomi-modena.sql";
    private static final String XIAOMI_STABLE_NAME = "xiaomi_modena";
    private static final String REAL_BENCHMARK_DB =
            TestUtils.camelToSnake(XiaomiLineVsFastSerializationManualTest.class) + "_real";

    private enum RealRouteMode {
        FAST,
        JDBC,
        LINE
    }

    @Test
    public void manualBenchmark_printsXiaomiLineVsFastComparison() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi=true to run Xiaomi manual benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi"));

        BenchmarkSummary summary = runBenchmark();
        assertTrue(summary.line.avgNanos > 0);
        assertTrue(summary.fast.avgNanos > 0);
        assertTrue(summary.line.requestBytes > 0);
        assertTrue(summary.fast.requestBytes > 0);
    }

    @Test
    public void serializers_produceRequestBytesForSameXiaomiRows() throws Exception {
        XiaomiSchema schema = loadSchema("/manual/xiaomi-modena.sql");
        XiaomiWorkload workload = new XiaomiWorkload(32, 4);

        ClientSerializationResult line = lineSerialization(schema, workload);
        ClientSerializationResult fast = fastSerialization(schema, workload);

        assertTrue(line.requestBytes > 0);
        assertTrue(fast.requestBytes > 0);
    }

    @Test
    public void extensionValue_isAtLeast400BytesWithinColumnLimit() throws Exception {
        XiaomiSchema schema = loadSchema(XIAOMI_DDL_RESOURCE);
        XiaomiColumn extension = null;
        for (XiaomiColumn column : schema.columns) {
            if ("extension".equalsIgnoreCase(column.name)) {
                extension = column;
                break;
            }
        }
        assertTrue(extension != null);

        String value = stringValue(extension, 0, 0);
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        assertTrue(bytes >= 400);
        assertTrue(bytes <= extension.field.getBytes());
    }

    @Test
    public void benchmarkOutput_includesExecuteIterationsWhenConfigured() throws Exception {
        String output = withBenchmarkProperty("ws.perf.manual.xiaomi.rows", "4", new CheckedSupplier<String>() {
            @Override
            public String get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.tables", "2", new CheckedSupplier<String>() {
                    @Override
                    public String get() throws Exception {
                        return withBenchmarkProperty("ws.perf.manual.xiaomi.warmup", "0", new CheckedSupplier<String>() {
                            @Override
                            public String get() throws Exception {
                                return withBenchmarkProperty("ws.perf.manual.xiaomi.rounds", "1", new CheckedSupplier<String>() {
                                    @Override
                                    public String get() throws Exception {
                                        return withBenchmarkProperty("ws.perf.manual.xiaomi.executes", "3", new CheckedSupplier<String>() {
                                            @Override
                                            public String get() throws Exception {
                                                return captureStdout(new CheckedSupplier<BenchmarkSummary>() {
                                                    @Override
                                                    public BenchmarkSummary get() throws Exception {
                                                        return runBenchmark();
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });

        assertTrue(output.contains("executes=3"));
    }

    @Test
    public void manualRealBenchmark_printsXiaomiRealComparison() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi.real=true to run Xiaomi real benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi.real"));

        RealBenchmarkSummary summary = runRealBenchmark();
        assertEquals(Integer.getInteger("ws.perf.manual.xiaomi.real.rowsPerBatch", 500).intValue(), summary.rowsPerBatch);
        List<RealRouteMode> routes = configuredRealRoutes();
        assertExecutedRouteSummary(summary.line, routes.contains(RealRouteMode.LINE));
        assertExecutedRouteSummary(summary.jdbc, routes.contains(RealRouteMode.JDBC));
        assertExecutedRouteSummary(summary.fast, routes.contains(RealRouteMode.FAST));
    }

    @Test
    public void manualRealBenchmark_defaultRoute_runsAllPaths() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi.real=true to run Xiaomi real benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi.real"));

        String output = withSmallRealBenchmarkProperties(new CheckedSupplier<String>() {
            @Override
            public String get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.keepDb", null, new CheckedSupplier<String>() {
                    @Override
                    public String get() throws Exception {
                        return captureStdout(new CheckedSupplier<RealBenchmarkSummary>() {
                            @Override
                            public RealBenchmarkSummary get() throws Exception {
                                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.route", null, new CheckedSupplier<RealBenchmarkSummary>() {
                                    @Override
                                    public RealBenchmarkSummary get() throws Exception {
                                        return runRealBenchmark();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });

        assertTrue(output.contains("line_mode"));
        assertTrue(output.contains("jdbc_columnar"));
        assertTrue(output.contains("fast_stmt2"));
    }

    @Test
    public void manualRealBenchmark_defaultRunCleansDatabase() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi.real=true to run Xiaomi real benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi.real"));

        withSmallRealBenchmarkProperties(new CheckedSupplier<Void>() {
            @Override
            public Void get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.keepDb", null, new CheckedSupplier<Void>() {
                    @Override
                    public Void get() throws Exception {
                        runRealBenchmark();
                        try (Connection conn = openRealConnection(RealRouteMode.FAST, null)) {
                            assertTrue(!databaseExists(conn, REAL_BENCHMARK_DB));
                        }
                        return null;
                    }
                });
            }
        });
    }

    @Test
    public void manualRealBenchmark_keepsDatabaseWhenConfigured() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi.real=true to run Xiaomi real benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi.real"));

        withBenchmarkProperty("ws.perf.manual.xiaomi.real.rowsPerBatch", "1", new CheckedSupplier<Void>() {
            @Override
            public Void get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.batches", "1", new CheckedSupplier<Void>() {
                    @Override
                    public Void get() throws Exception {
                        return withBenchmarkProperty("ws.perf.manual.xiaomi.real.tables", "1", new CheckedSupplier<Void>() {
                            @Override
                            public Void get() throws Exception {
                                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.route", "fast", new CheckedSupplier<Void>() {
                                    @Override
                                    public Void get() throws Exception {
                                        return withBenchmarkProperty("ws.perf.manual.xiaomi.real.keepDb", "true", new CheckedSupplier<Void>() {
                                            @Override
                                            public Void get() throws Exception {
                                                try {
                                                    runRealBenchmark();
                                                    try (Connection conn = openRealConnection(RealRouteMode.FAST, null)) {
                                                        assertTrue(databaseExists(conn, REAL_BENCHMARK_DB));
                                                    }
                                                } finally {
                                                    try (Connection conn = openRealConnection(RealRouteMode.FAST, null)) {
                                                        cleanupRealDatabase(conn);
                                                    }
                                                }
                                                return null;
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void manualRealBenchmark_routeFast_runsOnlyFastPath() throws Exception {
        Assume.assumeTrue("Set -Dws.perf.manual.xiaomi.real=true to run Xiaomi real benchmark",
                Boolean.getBoolean("ws.perf.manual.xiaomi.real"));

        String output = withBenchmarkProperty("ws.perf.manual.xiaomi.real.rowsPerBatch", "1", new CheckedSupplier<String>() {
            @Override
            public String get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.batches", "1", new CheckedSupplier<String>() {
                    @Override
                    public String get() throws Exception {
                        return withBenchmarkProperty("ws.perf.manual.xiaomi.real.tables", "1", new CheckedSupplier<String>() {
                            @Override
                            public String get() throws Exception {
                                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.route", "fast", new CheckedSupplier<String>() {
                                    @Override
                                    public String get() throws Exception {
                                        return captureStdout(new CheckedSupplier<RealBenchmarkSummary>() {
                                            @Override
                                            public RealBenchmarkSummary get() throws Exception {
                                                return runRealBenchmark();
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });

        assertTrue(output.contains("fast_stmt2"));
        assertTrue(!output.contains("line_mode"));
        assertTrue(!output.contains("jdbc_columnar"));
    }

    private static XiaomiSchema loadSchema(String resourcePath) throws Exception {
        return XiaomiDdlParser.parse(loadResourceText(resourcePath));
    }

    private static String loadResourceText(String resourcePath) throws Exception {
        try (InputStream in = XiaomiLineVsFastSerializationManualTest.class.getResourceAsStream(resourcePath)) {
            return new String(readAllBytes(Objects.requireNonNull(in, "missing resource")), StandardCharsets.UTF_8);
        }
    }

    private static byte[] readAllBytes(InputStream inputStream) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[4096];
        int bytesRead;
        while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, bytesRead);
        }
        return buffer.toByteArray();
    }

    private static <T> T withBenchmarkProperty(String key, String value, CheckedSupplier<T> supplier) throws Exception {
        String previous = System.getProperty(key);
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
        try {
            return supplier.get();
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    private static <T> T withSmallRealBenchmarkProperties(CheckedSupplier<T> supplier) throws Exception {
        return withBenchmarkProperty("ws.perf.manual.xiaomi.real.rowsPerBatch", "1", new CheckedSupplier<T>() {
            @Override
            public T get() throws Exception {
                return withBenchmarkProperty("ws.perf.manual.xiaomi.real.batches", "1", new CheckedSupplier<T>() {
                    @Override
                    public T get() throws Exception {
                        return withBenchmarkProperty("ws.perf.manual.xiaomi.real.tables", "1", supplier);
                    }
                });
            }
        });
    }

    private static void assertExecutedRouteSummary(RouteBenchmarkSummary summary, boolean expected) {
        if (expected) {
            assertTrue(summary != null);
            assertTrue(summary.elapsedNanos > 0);
        } else {
            assertTrue(summary == null);
        }
    }

    private static String captureStdout(CheckedSupplier<?> supplier) throws Exception {
        java.io.PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        java.io.PrintStream capture = new java.io.PrintStream(buffer, true, "UTF-8");
        System.setOut(capture);
        try {
            supplier.get();
        } finally {
            System.setOut(original);
            capture.close();
        }
        return buffer.toString("UTF-8");
    }

    private static BenchmarkSummary runBenchmark() throws Exception {
        int rows = Integer.getInteger("ws.perf.manual.xiaomi.rows", 64);
        int tables = Integer.getInteger("ws.perf.manual.xiaomi.tables", 16);
        int warmup = Integer.getInteger("ws.perf.manual.xiaomi.warmup", 2);
        int rounds = Integer.getInteger("ws.perf.manual.xiaomi.rounds", 5);
        int executes = Integer.getInteger("ws.perf.manual.xiaomi.executes", 100);
        if (rounds <= 0) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.rounds must be > 0");
        }
        if (executes <= 0) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.executes must be > 0");
        }

        XiaomiSchema schema = loadSchema("/manual/xiaomi-modena.sql");
        XiaomiWorkload workload = new XiaomiWorkload(rows, tables);
        ModeSummary line;
        ModeSummary fast;
        try (ReusableSerializer lineSerializer = new LineSerializerSession(schema);
             ReusableSerializer fastSerializer = new FastSerializerSession(schema)) {
            for (int i = 0; i < warmup; i++) {
                if ((i & 1) == 0) {
                    runExecutions(lineSerializer, workload, executes);
                    runExecutions(fastSerializer, workload, executes);
                } else {
                    runExecutions(fastSerializer, workload, executes);
                    runExecutions(lineSerializer, workload, executes);
                }
            }

            long lineTotal = 0L;
            long fastTotal = 0L;
            int lineBytes = 0;
            int fastBytes = 0;
            for (int i = 0; i < rounds; i++) {
                if ((i & 1) == 0) {
                    TimedResult lineRound = timedExecutions(lineSerializer, workload, executes);
                    lineTotal += lineRound.elapsedNanos;
                    lineBytes = lineRound.result.requestBytes;

                    TimedResult fastRound = timedExecutions(fastSerializer, workload, executes);
                    fastTotal += fastRound.elapsedNanos;
                    fastBytes = fastRound.result.requestBytes;
                } else {
                    TimedResult fastRound = timedExecutions(fastSerializer, workload, executes);
                    fastTotal += fastRound.elapsedNanos;
                    fastBytes = fastRound.result.requestBytes;

                    TimedResult lineRound = timedExecutions(lineSerializer, workload, executes);
                    lineTotal += lineRound.elapsedNanos;
                    lineBytes = lineRound.result.requestBytes;
                }
            }

            long measuredExecutions = (long) rounds * executes;
            line = new ModeSummary(lineTotal / measuredExecutions, lineBytes);
            fast = new ModeSummary(fastTotal / measuredExecutions, fastBytes);
        }

        BenchmarkSummary summary = new BenchmarkSummary(line, fast);
        double lineMs = line.avgNanos / 1_000_000.0d;
        double fastMs = fast.avgNanos / 1_000_000.0d;
        double lineNsPerRow = (double) line.avgNanos / rows;
        double fastNsPerRow = (double) fast.avgNanos / rows;
        double fastOverLine = (double) fast.avgNanos / (double) line.avgNanos;
        double speedup = (double) line.avgNanos / (double) fast.avgNanos;
        System.out.printf(
                "[XiaomiSerialize] columns=%d rows=%d tables=%d warmup=%d rounds=%d executes=%d line=%.2f ms/exec (%.2f ns/row, %d bytes) fast=%.2f ms/exec (%.2f ns/row, %d bytes) fast/line=%.3fx speedup=%.3fx%n",
                schema.columns.size(),
                rows,
                tables,
                warmup,
                rounds,
                executes,
                lineMs,
                lineNsPerRow,
                line.requestBytes,
                fastMs,
                fastNsPerRow,
                fast.requestBytes,
                fastOverLine,
                speedup);
        return summary;
    }

    private static RealBenchmarkSummary runRealBenchmark() throws Exception {
        int rowsPerBatch = Integer.getInteger("ws.perf.manual.xiaomi.real.rowsPerBatch", 500);
        int batchCount = Integer.getInteger("ws.perf.manual.xiaomi.real.batches", 10);
        int subTableCount = Integer.getInteger("ws.perf.manual.xiaomi.real.tables", rowsPerBatch);
        if (rowsPerBatch <= 0) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.real.rowsPerBatch must be > 0");
        }
        if (batchCount <= 0) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.real.batches must be > 0");
        }
        if (subTableCount <= 0) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.real.tables must be > 0");
        }

        assumeStmt2BindExecSupported();
        XiaomiSchema schema = loadSchema(XIAOMI_DDL_RESOURCE);
        String ddl = loadResourceText(XIAOMI_DDL_RESOURCE);
        List<RealRouteMode> routes = configuredRealRoutes();
        boolean cleanup = shouldCleanupRealDatabase();
        try {
            RouteBenchmarkSummary line = null;
            RouteBenchmarkSummary jdbc = null;
            RouteBenchmarkSummary fast = null;
            for (RealRouteMode route : routes) {
                RouteBenchmarkSummary routeSummary =
                        runRealPath(route, realRouteLabel(route), schema, ddl, rowsPerBatch, batchCount, subTableCount);
                switch (route) {
                    case LINE:
                        line = routeSummary;
                        break;
                    case JDBC:
                        jdbc = routeSummary;
                        break;
                    case FAST:
                        fast = routeSummary;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported route mode: " + route);
                }
            }
            RealBenchmarkSummary summary = new RealBenchmarkSummary(line, jdbc, fast, rowsPerBatch, batchCount);
            printRealSummary(summary, schema.columns.size(), subTableCount);
            if (!cleanup) {
                System.out.printf("[XiaomiRealPerf] kept_db=%s%n", REAL_BENCHMARK_DB);
            }
            return summary;
        } finally {
            if (cleanup) {
                try (Connection conn = openRealConnection(RealRouteMode.FAST, null)) {
                    cleanupRealDatabase(conn);
                }
            }
        }
    }

    private static List<RealRouteMode> configuredRealRoutes() {
        String raw = System.getProperty("ws.perf.manual.xiaomi.real.route");
        List<RealRouteMode> routes = new ArrayList<RealRouteMode>();
        if (raw == null || raw.trim().isEmpty() || "all".equalsIgnoreCase(raw.trim())) {
            routes.add(RealRouteMode.LINE);
            routes.add(RealRouteMode.JDBC);
            routes.add(RealRouteMode.FAST);
            return routes;
        }
        for (String token : raw.split(",")) {
            String route = token.trim();
            if (route.isEmpty()) {
                continue;
            }
            RealRouteMode mode;
            if ("line".equalsIgnoreCase(route) || "line_mode".equalsIgnoreCase(route)) {
                mode = RealRouteMode.LINE;
            } else if ("jdbc".equalsIgnoreCase(route) || "jdbc_columnar".equalsIgnoreCase(route)) {
                mode = RealRouteMode.JDBC;
            } else if ("fast".equalsIgnoreCase(route) || "fast_stmt2".equalsIgnoreCase(route)) {
                mode = RealRouteMode.FAST;
            } else {
                throw new IllegalArgumentException("Unsupported ws.perf.manual.xiaomi.real.route: " + raw);
            }
            if (!routes.contains(mode)) {
                routes.add(mode);
            }
        }
        if (routes.isEmpty()) {
            throw new IllegalArgumentException("ws.perf.manual.xiaomi.real.route must not be empty");
        }
        return routes;
    }

    private static boolean shouldCleanupRealDatabase() {
        return !Boolean.getBoolean("ws.perf.manual.xiaomi.real.keepDb");
    }

    private static String realRouteLabel(RealRouteMode routeMode) {
        switch (routeMode) {
            case LINE:
                return "line_mode";
            case JDBC:
                return "jdbc_columnar";
            case FAST:
                return "fast_stmt2";
            default:
                throw new IllegalArgumentException("Unsupported route mode: " + routeMode);
        }
    }

    private static void assumeStmt2BindExecSupported() throws SQLException {
        try (Connection conn = openRealConnection(RealRouteMode.FAST, null)) {
            Assume.assumeTrue("Benchmark requires WSConnection", conn instanceof WSConnection);
            Assume.assumeTrue(
                    "Benchmark requires a stmt2_bind_exec capable server",
                    ((WSConnection) conn).supportsStmt2BindExec());
        }
    }

    private static String buildRealWsUrl(RealRouteMode routeMode, String database) {
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
        url.append("?user=").append(TestEnvUtil.getUser())
                .append("&password=").append(TestEnvUtil.getPassword());
        switch (routeMode) {
            case LINE:
                url.append("&pbsMode=line");
                break;
            case JDBC:
                url.append("&").append(TSDBDriver.PROPERTY_KEY_STMT2_BIND_MODE).append("=jdbc");
                break;
            case FAST:
                url.append("&").append(TSDBDriver.PROPERTY_KEY_STMT2_BIND_MODE).append("=fast");
                break;
            default:
                throw new IllegalArgumentException("Unsupported route mode: " + routeMode);
        }
        return url.toString();
    }

    private static Connection openRealConnection(RealRouteMode routeMode, String database) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "false");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "false");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RETRY_TIMES, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, String.valueOf(600_000));
        return DriverManager.getConnection(buildRealWsUrl(routeMode, database), properties);
    }

    private static RouteBenchmarkSummary runRealPath(
            RealRouteMode routeMode,
            String label,
            XiaomiSchema schema,
            String ddl,
            int rowsPerBatch,
            int batchCount,
            int subTableCount) throws Exception {
        try (Connection adminConn = openRealConnection(routeMode, null)) {
            recreateRealSchema(adminConn, ddl, subTableCount);
        }
        try (Connection conn = openRealConnection(routeMode, REAL_BENCHMARK_DB)) {
            try {
                long insertedRows = 0L;
                long totalBatchNs = 0L;
                try (PreparedStatement pstmt = conn.prepareStatement(schema.insertSql())) {
                    assertRealRoute(pstmt, routeMode);
                    long started = System.nanoTime();
                    for (int batch = 0; batch < batchCount; batch++) {
                        long batchStart = System.nanoTime();
                        bindRealBatch(pstmt, schema, (long) batch * rowsPerBatch, rowsPerBatch, subTableCount);
                        int[] results = pstmt.executeBatch();
                        assertEquals(rowsPerBatch, results.length);
                        insertedRows += rowsPerBatch;
                        totalBatchNs += System.nanoTime() - batchStart;
                    }
                    long elapsedNs = System.nanoTime() - started;
                    verifyRealCounts(conn, (long) rowsPerBatch * batchCount, subTableCount);
                    return new RouteBenchmarkSummary(label, elapsedNs, insertedRows, totalBatchNs / batchCount);
                }
            } finally {
                cleanupRealDatabase(conn);
            }
        }
    }

    private static void recreateRealSchema(Connection conn, String ddl, int subTableCount) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + REAL_BENCHMARK_DB);
            stmt.execute("create database " + REAL_BENCHMARK_DB);
            stmt.execute("use " + REAL_BENCHMARK_DB);
            stmt.execute(toBenchmarkStableDdl(ddl));

            StringBuilder sql = new StringBuilder("create table");
            for (int i = 0; i < subTableCount; i++) {
                String tableName = realTableNameFor(i, subTableCount);
                sql.append(" if not exists ")
                        .append(tableName)
                        .append(" using ")
                        .append(XIAOMI_STABLE_NAME)
                        .append(" tags('")
                        .append(tableName)
                        .append("')");
                if ((i + 1) % 50 == 0) {
                    stmt.execute(sql.toString());
                    sql = new StringBuilder("create table");
                }
            }
            if (sql.length() > "create table".length()) {
                stmt.execute(sql.toString());
            }
        }
    }

    private static String toBenchmarkStableDdl(String ddl) {
        String rewritten = ddl.replaceFirst("(?i)CREATE\\s+STABLE\\s+[^\\s(]+\\s*\\(",
                "CREATE STABLE " + XIAOMI_STABLE_NAME + " (");
        if (rewritten.trim().endsWith(";")) {
            return rewritten.trim();
        }
        return rewritten.trim() + ";";
    }

    private static void cleanupRealDatabase(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + REAL_BENCHMARK_DB);
        } catch (SQLException ignored) {
        }
    }

    private static void assertRealRoute(PreparedStatement pstmt, RealRouteMode routeMode) {
        switch (routeMode) {
            case LINE:
                assertTrue("Expected WSRowPreparedStatement, got " + pstmt.getClass().getName(),
                        pstmt instanceof WSRowPreparedStatement);
                return;
            case JDBC:
                assertEquals("Expected exact WSColumnPreparedStatement, got " + pstmt.getClass().getName(),
                        WSColumnPreparedStatement.class, pstmt.getClass());
                return;
            case FAST:
                assertTrue("Expected WSColumnFastPreparedStatement, got " + pstmt.getClass().getName(),
                        pstmt instanceof WSColumnFastPreparedStatement);
                return;
            default:
                throw new AssertionError("Unsupported route mode: " + routeMode);
        }
    }

    private static void bindRealBatch(
            PreparedStatement pstmt,
            XiaomiSchema schema,
            long startRow,
            int rows,
            int subTableCount) throws Exception {
        for (int row = 0; row < rows; row++) {
            int actualRow = (int) (startRow + row);
            pstmt.setString(1, realTableNameFor(actualRow, subTableCount));
            for (int columnIndex = 0; columnIndex < schema.columns.size(); columnIndex++) {
                bindValue(pstmt, columnIndex + 2, schema.columns.get(columnIndex), actualRow, columnIndex);
            }
            pstmt.addBatch();
        }
    }

    private static String realTableNameFor(long rowIndex, int subTableCount) {
        return "xm_" + String.format("%04d", rowIndex % subTableCount);
    }

    private static void verifyRealCounts(Connection conn, long expectedRows, int expectedTables) throws SQLException {
        assertEquals(expectedRows, queryCount(conn, "select count(*) from " + XIAOMI_STABLE_NAME));
        assertEquals(expectedTables, countTables(conn));
    }

    private static long queryCount(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static boolean databaseExists(Connection conn, String database) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("show databases")) {
            while (rs.next()) {
                if (database.equalsIgnoreCase(rs.getString(1))) {
                    return true;
                }
            }
            return false;
        }
    }

    private static int countTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("show tables")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    private static long rowsPerSecond(RouteBenchmarkSummary summary) {
        if (summary.elapsedNanos <= 0L) {
            return 0L;
        }
        return Math.round(summary.insertedRows * 1_000_000_000.0d / summary.elapsedNanos);
    }

    private static void printRealSummary(RealBenchmarkSummary summary, int columnCount, int subTableCount) {
        printRealRouteSummary(summary.line, summary.rowsPerBatch, summary.batchCount, subTableCount, columnCount);
        printRealRouteSummary(summary.jdbc, summary.rowsPerBatch, summary.batchCount, subTableCount, columnCount);
        printRealRouteSummary(summary.fast, summary.rowsPerBatch, summary.batchCount, subTableCount, columnCount);
        if (summary.line != null && (summary.jdbc != null || summary.fast != null)) {
            StringBuilder baseline = new StringBuilder("[XiaomiRealPerf] line_baseline");
            if (summary.jdbc != null) {
                baseline.append(" jdbc_vs_line=")
                        .append(String.format("%.3f", (double) summary.line.elapsedNanos / (double) summary.jdbc.elapsedNanos));
            }
            if (summary.fast != null) {
                baseline.append(" fast_vs_line=")
                        .append(String.format("%.3f", (double) summary.line.elapsedNanos / (double) summary.fast.elapsedNanos));
            }
            System.out.println(baseline.toString());
        }
    }

    private static void printRealRouteSummary(
            RouteBenchmarkSummary summary,
            int rowsPerBatch,
            int batchCount,
            int subTableCount,
            int columnCount) {
        if (summary == null) {
            return;
        }
        System.out.printf(
                "[XiaomiRealPerf] %s total=%d elapsedMs=%.2f rowsPerSec=%d avgBatchMs=%.2f rowsPerBatch=%d batches=%d tables=%d columns=%d%n",
                summary.label,
                summary.insertedRows,
                summary.elapsedNanos / 1_000_000.0d,
                rowsPerSecond(summary),
                summary.avgBatchNanos / 1_000_000.0d,
                rowsPerBatch,
                batchCount,
                subTableCount,
                columnCount);
    }

    private static TimedResult timedExecutions(
            ReusableSerializer serializer,
            XiaomiWorkload workload,
            int executes) throws Exception {
        long start = System.nanoTime();
        ClientSerializationResult result = runExecutions(serializer, workload, executes);
        return new TimedResult(System.nanoTime() - start, result);
    }

    private static ClientSerializationResult lineSerialization(XiaomiSchema schema, XiaomiWorkload workload) throws Exception {
        try (ReusableSerializer serializer = new LineSerializerSession(schema)) {
            return serializer.serialize(workload);
        }
    }

    private static ClientSerializationResult fastSerialization(XiaomiSchema schema, XiaomiWorkload workload) throws Exception {
        try (ReusableSerializer serializer = new FastSerializerSession(schema)) {
            return serializer.serialize(workload);
        }
    }

    private static ClientSerializationResult runExecutions(
            ReusableSerializer serializer,
            XiaomiWorkload workload,
            int executes) throws Exception {
        ClientSerializationResult result = null;
        for (int i = 0; i < executes; i++) {
            result = serializer.serialize(workload);
        }
        return result;
    }

    private static void bindWorkload(PreparedStatement stmt, XiaomiSchema schema, XiaomiWorkload workload) throws Exception {
        for (int row = 0; row < workload.rows; row++) {
            stmt.setString(1, workload.tableName(row));
            for (int columnIndex = 0; columnIndex < schema.columns.size(); columnIndex++) {
                bindValue(stmt, columnIndex + 2, schema.columns.get(columnIndex), row, columnIndex);
            }
            stmt.addBatch();
        }
    }

    private static void bindValue(PreparedStatement stmt, int parameterIndex, XiaomiColumn column,
                                  int rowIndex, int columnIndex) throws Exception {
        byte fieldType = column.field.getFieldType();
        if (fieldType == TSDB_DATA_TYPE_TIMESTAMP) {
            stmt.setTimestamp(parameterIndex, new Timestamp(BASE_TS + rowIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_BIGINT) {
            stmt.setLong(parameterIndex, signedLongValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_UBIGINT) {
            stmt.setObject(parameterIndex, BigInteger.valueOf(unsignedIntValue(rowIndex, columnIndex)));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_INT) {
            stmt.setInt(parameterIndex, signedIntValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_UINT) {
            stmt.setLong(parameterIndex, unsignedIntValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_SMALLINT) {
            stmt.setShort(parameterIndex, signedShortValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_USMALLINT) {
            stmt.setInt(parameterIndex, unsignedShortValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_TINYINT) {
            stmt.setByte(parameterIndex, signedByteValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_UTINYINT) {
            stmt.setShort(parameterIndex, unsignedByteValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_DOUBLE) {
            stmt.setDouble(parameterIndex, floatingValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_FLOAT) {
            stmt.setFloat(parameterIndex, (float) floatingValue(rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_BOOL) {
            stmt.setBoolean(parameterIndex, ((rowIndex + columnIndex) & 1) == 0);
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_DECIMAL64 || fieldType == TSDB_DATA_TYPE_DECIMAL128) {
            stmt.setBigDecimal(parameterIndex,
                    BigDecimal.valueOf(unsignedIntValue(rowIndex, columnIndex), Math.max(1, (int) column.field.getScale())));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_VARBINARY || fieldType == TSDB_DATA_TYPE_GEOMETRY) {
            stmt.setBytes(parameterIndex, binaryValue(column, rowIndex, columnIndex));
            return;
        }
        if (fieldType == TSDB_DATA_TYPE_VARCHAR || fieldType == TSDB_DATA_TYPE_NCHAR) {
            stmt.setString(parameterIndex, stringValue(column, rowIndex, columnIndex));
            return;
        }
        throw new IllegalArgumentException("Unsupported bind field type: " + fieldType + " for " + column.typeExpr);
    }

    private static ConnectionParam mockParam() {
        ConnectionParam param = mock(ConnectionParam.class);
        when(param.getRequestTimeout()).thenReturn(30_000);
        when(param.getZoneId()).thenReturn(null);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getDatabase()).thenReturn("testdb");
        return param;
    }

    private static Transport mockTransport(ConnectionParam param) {
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);
        when(transport.isConnected()).thenReturn(true);
        when(transport.isClosed()).thenReturn(false);
        when(transport.getConnectionParam()).thenReturn(param);
        return transport;
    }

    private static Stmt2PayloadAccess stmt2PayloadAccess(Object stmt, Class<?> stmtClass) throws Exception {
        try {
            Method buildPayloadBuffer = stmtClass.getDeclaredMethod("buildPayloadBuffer");
            buildPayloadBuffer.setAccessible(true);
            Method resetFastState = stmtClass.getDeclaredMethod("resetFastState");
            resetFastState.setAccessible(true);
            return new Stmt2PayloadAccess(stmt, buildPayloadBuffer, resetFastState);
        } catch (NoSuchMethodException ignored) {
            java.lang.reflect.Field batchStateField = stmtClass.getDeclaredField("batchState");
            batchStateField.setAccessible(true);
            Object batchState = batchStateField.get(stmt);
            Method buildPayloadBuffer = batchState.getClass().getDeclaredMethod("buildPayloadBuffer");
            buildPayloadBuffer.setAccessible(true);
            Method reset = batchState.getClass().getDeclaredMethod("reset");
            reset.setAccessible(true);
            return new Stmt2PayloadAccess(batchState, buildPayloadBuffer, reset);
        }
    }

    private static RowPayloadAccess rowPayloadAccess(WSRowPreparedStatement stmt) throws Exception {
        Method buffersStopWrite = WSRowPreparedStatement.class.getDeclaredMethod("buffersStopWrite");
        buffersStopWrite.setAccessible(true);
        Method getRawBlock = WSRowPreparedStatement.class.getDeclaredMethod("getRawBlock");
        getRawBlock.setAccessible(true);
        Method initBuffers = WSRowPreparedStatement.class.getDeclaredMethod("initBuffers");
        initBuffers.setAccessible(true);
        java.lang.reflect.Field totalTableCount = WSRowPreparedStatement.class.getDeclaredField("totalTableCount");
        totalTableCount.setAccessible(true);
        return new RowPayloadAccess(stmt, buffersStopWrite, getRawBlock, initBuffers, totalTableCount);
    }

    private static byte signedByteValue(int rowIndex, int columnIndex) {
        return (byte) ((rowIndex + columnIndex) % 120);
    }

    private static short unsignedByteValue(int rowIndex, int columnIndex) {
        return (short) ((rowIndex + columnIndex) % 250);
    }

    private static short signedShortValue(int rowIndex, int columnIndex) {
        return (short) ((rowIndex * 13 + columnIndex) % 30_000);
    }

    private static int unsignedShortValue(int rowIndex, int columnIndex) {
        return (rowIndex * 17 + columnIndex) % 60_000;
    }

    private static int signedIntValue(int rowIndex, int columnIndex) {
        return rowIndex * 97 + columnIndex;
    }

    private static long unsignedIntValue(int rowIndex, int columnIndex) {
        return rowIndex * 1_003L + columnIndex;
    }

    private static long signedLongValue(int rowIndex, int columnIndex) {
        return rowIndex * 10_000L + columnIndex;
    }

    private static double floatingValue(int rowIndex, int columnIndex) {
        return rowIndex + (columnIndex / 1000.0d);
    }

    private static String stringValue(XiaomiColumn column, int rowIndex, int columnIndex) {
        int maxBytes = Math.max(4, column.field.getBytes());
        String base = column.name + "_" + rowIndex + "_" + columnIndex;
        if ("extension".equalsIgnoreCase(column.name)) {
            return fillAsciiString(base, Math.min(maxBytes, EXTENSION_MIN_BYTES));
        }
        return fillAsciiString(base, maxBytes);
    }

    private static String fillAsciiString(String seed, int targetBytes) {
        if (seed.length() >= targetBytes) {
            return seed.substring(0, targetBytes);
        }
        StringBuilder builder = new StringBuilder(targetBytes);
        while (builder.length() < targetBytes) {
            if (builder.length() > 0) {
                builder.append('|');
            }
            builder.append(seed);
        }
        if (builder.length() > targetBytes) {
            builder.setLength(targetBytes);
        }
        return builder.toString();
    }

    private static byte[] binaryValue(XiaomiColumn column, int rowIndex, int columnIndex) {
        byte[] bytes = stringValue(column, rowIndex, columnIndex).getBytes(StandardCharsets.UTF_8);
        int maxBytes = Math.max(1, column.field.getBytes());
        if (bytes.length <= maxBytes) {
            return bytes;
        }
        byte[] truncated = new byte[maxBytes];
        System.arraycopy(bytes, 0, truncated, 0, maxBytes);
        return truncated;
    }

    private static final class XiaomiSchema {
        private final List<XiaomiColumn> columns;
        private final List<Field> fields;

        private XiaomiSchema(List<XiaomiColumn> columns, List<Field> fields) {
            this.columns = columns;
            this.fields = fields;
        }

        private Stmt2PrepareResp prepareResp() {
            Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
            prepareResp.setCode(0);
            prepareResp.setInsert(true);
            prepareResp.setStmtId(0L);
            prepareResp.setFields(fields);
            prepareResp.setFieldsCount(fields.size());
            return prepareResp;
        }

        private String insertSql() {
            StringBuilder columnsSql = new StringBuilder("tbname");
            StringBuilder valuesSql = new StringBuilder("?");
            for (XiaomiColumn column : columns) {
                columnsSql.append(", ").append(column.name);
                valuesSql.append(", ?");
            }
            return "insert into xiaomi_modena (" + columnsSql + ") values (" + valuesSql + ")";
        }
    }

    private static final class XiaomiWorkload {
        private final int rows;
        private final int tableCount;

        private XiaomiWorkload(int rows, int tableCount) {
            this.rows = rows;
            this.tableCount = tableCount;
        }

        private String tableName(int rowIndex) {
            return "xm_" + (rowIndex % tableCount);
        }
    }

    private static final class ClientSerializationResult {
        private final int requestBytes;

        private ClientSerializationResult(int requestBytes) {
            this.requestBytes = requestBytes;
        }
    }

    private interface ReusableSerializer extends AutoCloseable {
        ClientSerializationResult serialize(XiaomiWorkload workload) throws Exception;

        @Override
        void close() throws Exception;
    }

    private static final class TimedResult {
        private final long elapsedNanos;
        private final ClientSerializationResult result;

        private TimedResult(long elapsedNanos, ClientSerializationResult result) {
            this.elapsedNanos = elapsedNanos;
            this.result = result;
        }
    }

    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    private static final class ModeSummary {
        private final long avgNanos;
        private final int requestBytes;

        private ModeSummary(long avgNanos, int requestBytes) {
            this.avgNanos = avgNanos;
            this.requestBytes = requestBytes;
        }
    }

    private static final class BenchmarkSummary {
        private final ModeSummary line;
        private final ModeSummary fast;

        private BenchmarkSummary(ModeSummary line, ModeSummary fast) {
            this.line = line;
            this.fast = fast;
        }
    }

    private static final class RouteBenchmarkSummary {
        private final String label;
        private final long elapsedNanos;
        private final long insertedRows;
        private final long avgBatchNanos;

        private RouteBenchmarkSummary(String label, long elapsedNanos, long insertedRows, long avgBatchNanos) {
            this.label = label;
            this.elapsedNanos = elapsedNanos;
            this.insertedRows = insertedRows;
            this.avgBatchNanos = avgBatchNanos;
        }
    }

    private static final class RealBenchmarkSummary {
        private final RouteBenchmarkSummary line;
        private final RouteBenchmarkSummary jdbc;
        private final RouteBenchmarkSummary fast;
        private final int rowsPerBatch;
        private final int batchCount;

        private RealBenchmarkSummary(
                RouteBenchmarkSummary line,
                RouteBenchmarkSummary jdbc,
                RouteBenchmarkSummary fast,
                int rowsPerBatch,
                int batchCount) {
            this.line = line;
            this.jdbc = jdbc;
            this.fast = fast;
            this.rowsPerBatch = rowsPerBatch;
            this.batchCount = batchCount;
        }
    }

    private static final class Stmt2PayloadAccess {
        private final Object owner;
        private final Method buildPayloadBuffer;
        private final Method reset;

        private Stmt2PayloadAccess(Object owner, Method buildPayloadBuffer, Method reset) {
            this.owner = owner;
            this.buildPayloadBuffer = buildPayloadBuffer;
            this.reset = reset;
        }

        private ByteBuf buildPayloadBuffer() throws Exception {
            return (ByteBuf) buildPayloadBuffer.invoke(owner);
        }

        private void reset() throws Exception {
            reset.invoke(owner);
        }
    }

    private static final class RowPayloadAccess {
        private final WSRowPreparedStatement stmt;
        private final Method buffersStopWrite;
        private final Method getRawBlock;
        private final Method initBuffers;
        private final java.lang.reflect.Field totalTableCount;

        private RowPayloadAccess(
                WSRowPreparedStatement stmt,
                Method buffersStopWrite,
                Method getRawBlock,
                Method initBuffers,
                java.lang.reflect.Field totalTableCount) {
            this.stmt = stmt;
            this.buffersStopWrite = buffersStopWrite;
            this.getRawBlock = getRawBlock;
            this.initBuffers = initBuffers;
            this.totalTableCount = totalTableCount;
        }

        private int requestBytes() throws Exception {
            buffersStopWrite.invoke(stmt);
            CompositeByteBuf rawBlock = (CompositeByteBuf) getRawBlock.invoke(stmt);
            try {
                return rawBlock.readableBytes();
            } finally {
                rawBlock.release();
            }
        }

        private void reset() throws Exception {
            initBuffers.invoke(stmt);
            totalTableCount.setInt(stmt, 0);
        }
    }

    private static final class LineSerializerSession implements ReusableSerializer {
        private final XiaomiSchema schema;
        private final WSRowPreparedStatement stmt;
        private final RowPayloadAccess access;

        private LineSerializerSession(XiaomiSchema schema) throws Exception {
            this.schema = schema;
            ConnectionParam param = mockParam();
            Transport transport = mockTransport(param);
            AbstractConnection conn = mock(AbstractConnection.class);
            this.stmt = new WSRowPreparedStatement(
                    transport, param, "testdb", conn, schema.insertSql(), 0L, schema.prepareResp());
            this.access = rowPayloadAccess(stmt);
        }

        @Override
        public ClientSerializationResult serialize(XiaomiWorkload workload) throws Exception {
            bindWorkload(stmt, schema, workload);
            try {
                return new ClientSerializationResult(access.requestBytes());
            } finally {
                access.reset();
            }
        }

        @Override
        public void close() throws Exception {
            stmt.close();
        }
    }

    private static final class FastSerializerSession implements ReusableSerializer {
        private final XiaomiSchema schema;
        private final WSColumnFastPreparedStatement stmt;
        private final Stmt2PayloadAccess access;

        private FastSerializerSession(XiaomiSchema schema) throws Exception {
            this.schema = schema;
            ConnectionParam param = mockParam();
            Transport transport = mockTransport(param);
            AbstractConnection conn = mock(AbstractConnection.class);
            this.stmt = new WSColumnFastPreparedStatement(
                    transport, param, "testdb", conn, schema.insertSql(), 0L, schema.prepareResp());
            this.access = stmt2PayloadAccess(stmt, WSColumnFastPreparedStatement.class);
        }

        @Override
        public ClientSerializationResult serialize(XiaomiWorkload workload) throws Exception {
            bindWorkload(stmt, schema, workload);
            ByteBuf payload = null;
            ByteBuf request = null;
            try {
                payload = access.buildPayloadBuffer();
                request = Stmt2BindExecRequestBuilder.build(payload);
                payload = null;
                return new ClientSerializationResult(request.readableBytes());
            } finally {
                if (request != null) {
                    Utils.releaseByteBuf(request);
                }
                if (payload != null) {
                    Utils.releaseByteBuf(payload);
                }
                access.reset();
            }
        }

        @Override
        public void close() throws Exception {
            stmt.close();
        }
    }

    private static final class XiaomiDdlParser {
        private static XiaomiSchema parse(String ddl) {
            List<XiaomiColumn> columns = new ArrayList<XiaomiColumn>();
            List<Field> fields = new ArrayList<Field>();
            fields.add(tbNameField());

            boolean inColumns = false;
            String[] lines = ddl.split("\\r?\\n");
            for (String rawLine : lines) {
                String line = rawLine.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (!inColumns) {
                    if (line.startsWith("CREATE STABLE") && line.endsWith("(")) {
                        inColumns = true;
                    }
                    continue;
                }
                if (line.startsWith(") TAGS")) {
                    break;
                }
                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1).trim();
                }
                int firstSpace = line.indexOf(' ');
                if (firstSpace <= 0) {
                    continue;
                }
                String name = line.substring(0, firstSpace).trim();
                String typeExpr = line.substring(firstSpace + 1).trim();
                XiaomiColumn column = XiaomiColumn.of(name, typeExpr);
                columns.add(column);
                fields.add(column.field);
            }
            return new XiaomiSchema(columns, fields);
        }

        private static Field tbNameField() {
            Field field = new Field();
            field.setName("tbname");
            field.setBindType((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue());
            field.setFieldType((byte) TSDB_DATA_TYPE_VARCHAR);
            field.setBytes(64);
            return field;
        }
    }

    private static final class XiaomiColumn {
        private final String name;
        private final String typeExpr;
        private final Field field;

        private XiaomiColumn(String name, String typeExpr, Field field) {
            this.name = name;
            this.typeExpr = typeExpr;
            this.field = field;
        }

        private static XiaomiColumn of(String name, String typeExpr) {
            Field field = new Field();
            field.setName(name);
            field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());

            String normalized = typeExpr.trim().toUpperCase();
            if (normalized.startsWith("TIMESTAMP")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
                field.setBytes(8);
            } else if (normalized.startsWith("BIGINT UNSIGNED")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_UBIGINT);
                field.setBytes(8);
            } else if (normalized.startsWith("INT UNSIGNED")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_UINT);
                field.setBytes(4);
            } else if (normalized.startsWith("SMALLINT UNSIGNED")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_USMALLINT);
                field.setBytes(2);
            } else if (normalized.startsWith("TINYINT UNSIGNED")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_UTINYINT);
                field.setBytes(1);
            } else if (normalized.startsWith("BIGINT")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_BIGINT);
                field.setBytes(8);
            } else if (normalized.startsWith("INT")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_INT);
                field.setBytes(4);
            } else if (normalized.startsWith("SMALLINT")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_SMALLINT);
                field.setBytes(2);
            } else if (normalized.startsWith("TINYINT")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_TINYINT);
                field.setBytes(1);
            } else if (normalized.startsWith("DOUBLE")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_DOUBLE);
                field.setBytes(8);
            } else if (normalized.startsWith("FLOAT")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_FLOAT);
                field.setBytes(4);
            } else if (normalized.startsWith("BOOL")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_BOOL);
                field.setBytes(1);
            } else if (normalized.startsWith("VARCHAR(") || normalized.startsWith("BINARY(")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_VARCHAR);
                field.setBytes(parseSingleIntArg(normalized));
            } else if (normalized.startsWith("NCHAR(")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_NCHAR);
                field.setBytes(parseSingleIntArg(normalized));
            } else if (normalized.startsWith("VARBINARY(")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_VARBINARY);
                field.setBytes(parseSingleIntArg(normalized));
            } else if (normalized.startsWith("GEOMETRY(")) {
                field.setFieldType((byte) TSDB_DATA_TYPE_GEOMETRY);
                field.setBytes(parseSingleIntArg(normalized));
            } else if (normalized.startsWith("DECIMAL(")) {
                int[] decimal = parseTwoIntArgs(normalized);
                field.setPrecision((byte) decimal[0]);
                field.setScale((byte) decimal[1]);
                field.setFieldType((byte) (decimal[0] <= 18 ? TSDB_DATA_TYPE_DECIMAL64 : TSDB_DATA_TYPE_DECIMAL128));
                field.setBytes(decimal[0]);
            } else {
                throw new IllegalArgumentException("Unsupported Xiaomi column type: " + typeExpr);
            }
            return new XiaomiColumn(name, typeExpr, field);
        }

        private static int parseSingleIntArg(String typeExpr) {
            int start = typeExpr.indexOf('(');
            int end = typeExpr.indexOf(')');
            return Integer.parseInt(typeExpr.substring(start + 1, end).trim());
        }

        private static int[] parseTwoIntArgs(String typeExpr) {
            int start = typeExpr.indexOf('(');
            int end = typeExpr.indexOf(')');
            String[] parts = typeExpr.substring(start + 1, end).split(",");
            return new int[]{Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim())};
        }
    }
}
