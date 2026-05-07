package com.taosdata.jdbc.ws.stmt;

import org.junit.Assume;
import org.junit.Test;

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
}
