package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import org.junit.Test;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Stmt2VariableWidthReuseHelperTest {

    @Test
    public void createReusableBuffer_bootstrapsAndPrimesCachedChunkCount() {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        WSEWChunkSizingUtil.BufferSpec spec = new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 3);

        Stmt2ColumnFieldBuffer buffer = Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(meta, spec);
        try {
            assertEquals(spec.getChunkBytes(), buffer.currentReusableSpec().getChunkBytes());
            assertEquals(spec.getReusableChunkCount(), buffer.currentReusableSpec().getReusableChunkCount());
            buffer.reset();
            assertEquals(spec.getChunkBytes(), buffer.currentReusableSpec().getChunkBytes());
            assertEquals(spec.getReusableChunkCount(), buffer.currentReusableSpec().getReusableChunkCount());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void resolveBufferSpec_fallsBackToBootstrapSpecForMissingInputs() {
        WSEWChunkSizingUtil.BufferSpec bootstrap = WSEWChunkSizingUtil.bootstrapSpec();

        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(null, 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(new WSEWChunkSizingUtil.BufferSpec[0], 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(
                        new WSEWChunkSizingUtil.BufferSpec[] {null}, 0));
    }

    @Test
    public void bufferSpecsEqual_comparesChunkBytesAndReusableCount() {
        assertTrue(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 2)));
    }

    @Test
    public void bufferSpecsEqual_handlesNullArguments() {
        WSEWChunkSizingUtil.BufferSpec spec = new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1);

        assertTrue(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, null));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, spec));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(spec, null));
    }

    @Test
    public void reactiveSizing_growsWhenOverflowOrTooManyChunks() {
        WSEWChunkSizingUtil.BufferSpec current = new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1);
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(80L * 1024, 1024, 100);
        stats.setActiveChunksUsed(9);
        stats.setOverflowCount(1);

        Stmt2VariableWidthReuseHelper.SizingDecision decision =
                Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, 0);

        assertTrue(decision.getNextSpec().getChunkBytes() > current.getChunkBytes());
        assertEquals(0, decision.getNextUnderuseStreak());
    }

    @Test
    public void reactiveSizing_throwsClearOverflowErrorWhenGrowthExceedsIntMax() {
        WSEWChunkSizingUtil.BufferSpec current = new WSEWChunkSizingUtil.BufferSpec(Integer.MAX_VALUE, 1);
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1L, 1, 1);
        stats.setOverflowCount(1);

        try {
            Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, 0);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("chunk size overflow"));
        }
    }

    @Test
    public void reactiveSizing_shrinksOnlyAfterHundredHalfUtilizedBatches() {
        WSEWChunkSizingUtil.BufferSpec current = new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 4);
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(20L * 1024, 128, 100);
        stats.setActiveChunksUsed(2);
        int threshold = WSEWChunkSizingUtil.SHRINK_STREAK_THRESHOLD;

        Stmt2VariableWidthReuseHelper.SizingDecision before =
                Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, threshold - 1);
        Stmt2VariableWidthReuseHelper.SizingDecision atThreshold =
                Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, threshold);

        assertEquals(current.getReusableChunkCount(), before.getNextSpec().getReusableChunkCount());
        assertEquals(current.getReusableChunkCount() - 1, atThreshold.getNextSpec().getReusableChunkCount());
    }

    private static void assertBootstrapSpec(WSEWChunkSizingUtil.BufferSpec expected,
            WSEWChunkSizingUtil.BufferSpec actual) {
        assertEquals(expected.getChunkBytes(), actual.getChunkBytes());
        assertEquals(expected.getReusableChunkCount(), actual.getReusableChunkCount());
    }
}
