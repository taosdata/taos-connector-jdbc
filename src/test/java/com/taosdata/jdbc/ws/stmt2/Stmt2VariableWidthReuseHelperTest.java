package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Stmt2VariableWidthReuseHelperTest {

    @Test
    public void createReusableBuffer_bootstrapsAndPrimesCachedChunkCount() {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        Stmt2ChunkSizingUtil.BufferSpec spec = new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 3);

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
    public void createReusableBuffer_keepsValuesThatFitChunkOnReusablePath() throws Exception {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        Stmt2ChunkSizingUtil.BufferSpec spec = new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 3);
        Stmt2ColumnFieldBuffer buffer = Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(meta, spec);
        try {
            String value = asciiString(12 * 1024);
            buffer.appendString(value);

            assertEquals(0, buffer.reusableOverflowCount());
            assertEquals(1, activeChunkCount(buffer));
            assertSame(cachedChunk(buffer, 0), activeChunkBuffer(buffer, 0));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void resolveBufferSpec_fallsBackToBootstrapSpecForMissingInputs() {
        Stmt2ChunkSizingUtil.BufferSpec bootstrap = Stmt2ChunkSizingUtil.bootstrapSpec();

        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(null, 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(new Stmt2ChunkSizingUtil.BufferSpec[0], 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(
                        new Stmt2ChunkSizingUtil.BufferSpec[] {null}, 0));
    }

    @Test
    public void bufferSpecsEqual_comparesChunkBytesAndReusableCount() {
        assertTrue(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 2)));
    }

    @Test
    public void bufferSpecsEqual_handlesNullArguments() {
        Stmt2ChunkSizingUtil.BufferSpec spec = new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1);

        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, null));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, spec));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(spec, null));
    }

    @Test
    public void createReusableBuffer_releasesOnPrimingFailure() throws Exception {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        Stmt2ChunkSizingUtil.BufferSpec spec = new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 3);
        Stmt2ColumnFieldBuffer realBuffer = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                meta, null, spec.getChunkBytes(), spec.getChunkBytes(), spec.getReusableChunkCount());
        Stmt2ColumnFieldBuffer spyBuffer = Mockito.spy(realBuffer);

        try (MockedStatic<Stmt2ColumnFieldBuffer> mocked = Mockito.mockStatic(Stmt2ColumnFieldBuffer.class)) {
            mocked.when(() -> Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                    meta, null, spec.getChunkBytes(), spec.getChunkBytes(), spec.getReusableChunkCount()))
                    .thenReturn(spyBuffer);
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                throw new OutOfMemoryError("boom");
            }).when(spyBuffer).primeReusableValueChunks(Mockito.anyInt());

            try {
                Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(meta, spec);
                fail("expected OutOfMemoryError");
            } catch (OutOfMemoryError expected) {
                // expected
            }
        }

        assertEquals(0, cachedChunkCount(spyBuffer));
    }

    @Test
    public void reactiveSizing_growsWhenOverflowOrTooManyChunks() {
        Stmt2ChunkSizingUtil.BufferSpec current = new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1);
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
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
        Stmt2ChunkSizingUtil.BufferSpec current = new Stmt2ChunkSizingUtil.BufferSpec(Integer.MAX_VALUE, 1);
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
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
        Stmt2ChunkSizingUtil.BufferSpec current = new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 4);
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(20L * 1024, 128, 100);
        stats.setActiveChunksUsed(2);
        int threshold = Stmt2ChunkSizingUtil.SHRINK_STREAK_THRESHOLD;

        Stmt2VariableWidthReuseHelper.SizingDecision before =
                Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, threshold - 1);
        Stmt2VariableWidthReuseHelper.SizingDecision atThreshold =
                Stmt2VariableWidthReuseHelper.reactiveDecision(current, stats, threshold);

        assertEquals(current.getReusableChunkCount(), before.getNextSpec().getReusableChunkCount());
        assertEquals(current.getReusableChunkCount() - 1, atThreshold.getNextSpec().getReusableChunkCount());
    }

    @Test
    public void reactiveSizing_atBootstrapFloor_keepsUnderuseStreak() {
        Stmt2ChunkSizingUtil.BufferSpec current = Stmt2ChunkSizingUtil.bootstrapSpec();
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(128, 128, 1);

        Stmt2VariableWidthReuseHelper.SizingDecision decision =
                Stmt2VariableWidthReuseHelper.reactiveDecision(
                        current, stats, Stmt2ChunkSizingUtil.SHRINK_STREAK_THRESHOLD);

        assertEquals(current.getChunkBytes(), decision.getNextSpec().getChunkBytes());
        assertEquals(current.getReusableChunkCount(), decision.getNextSpec().getReusableChunkCount());
        assertEquals(Stmt2ChunkSizingUtil.SHRINK_STREAK_THRESHOLD, decision.getNextUnderuseStreak());
    }

    private static void assertBootstrapSpec(Stmt2ChunkSizingUtil.BufferSpec expected,
                                            Stmt2ChunkSizingUtil.BufferSpec actual) {
        assertEquals(expected.getChunkBytes(), actual.getChunkBytes());
        assertEquals(expected.getReusableChunkCount(), actual.getReusableChunkCount());
    }

    private static int cachedChunkCount(Stmt2ColumnFieldBuffer buffer) throws Exception {
        java.lang.reflect.Field reusableField = Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Object reusable = reusableField.get(buffer);
        java.lang.reflect.Field cachedField = reusable.getClass().getDeclaredField("cachedStandardChunks");
        cachedField.setAccessible(true);
        return ((java.util.List<?>) cachedField.get(reusable)).size();
    }

    private static int activeChunkCount(Stmt2ColumnFieldBuffer buffer) throws Exception {
        java.lang.reflect.Field reusableField = Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Object reusable = reusableField.get(buffer);
        java.lang.reflect.Field activeField = reusable.getClass().getDeclaredField("activeChunks");
        activeField.setAccessible(true);
        return ((java.util.List<?>) activeField.get(reusable)).size();
    }

    private static Object cachedChunk(Stmt2ColumnFieldBuffer buffer, int index) throws Exception {
        java.lang.reflect.Field reusableField = Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Object reusable = reusableField.get(buffer);
        java.lang.reflect.Field cachedField = reusable.getClass().getDeclaredField("cachedStandardChunks");
        cachedField.setAccessible(true);
        return ((java.util.List<?>) cachedField.get(reusable)).get(index);
    }

    private static Object activeChunkBuffer(Stmt2ColumnFieldBuffer buffer, int index) throws Exception {
        java.lang.reflect.Field reusableField = Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Object reusable = reusableField.get(buffer);
        java.lang.reflect.Field activeField = reusable.getClass().getDeclaredField("activeChunks");
        activeField.setAccessible(true);
        Object chunkRef = ((java.util.List<?>) activeField.get(reusable)).get(index);
        java.lang.reflect.Field bufField = chunkRef.getClass().getDeclaredField("buf");
        bufField.setAccessible(true);
        return bufField.get(chunkRef);
    }

    private static String asciiString(int length) {
        char[] chars = new char[length];
        java.util.Arrays.fill(chars, 'a');
        return new String(chars);
    }
}
