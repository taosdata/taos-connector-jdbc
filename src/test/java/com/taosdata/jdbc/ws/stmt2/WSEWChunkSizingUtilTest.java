package com.taosdata.jdbc.ws.stmt2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WSEWChunkSizingUtilTest {

    @Test
    public void bootstrapSpec_defaultsTo8KbSingleChunk() {
        Stmt2ChunkSizingUtil.BufferSpec spec = Stmt2ChunkSizingUtil.bootstrapSpec();

        assertEquals(8 * 1024, spec.getChunkBytes());
        assertEquals(1, spec.getReusableChunkCount());
    }

    @Test
    public void deriveWantedSpec_projectsObservedBytesToFullBatch() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024, 400, 500);

        Stmt2ChunkSizingUtil.BufferSpec spec =
                Stmt2ChunkSizingUtil.deriveWantedSpec(stats, 2000);

        assertEquals(256 * 1024, spec.getChunkBytes());
        assertEquals(4, spec.getReusableChunkCount());
    }

    @Test
    public void canReuse_rejectsSmallChunksEvenWhenTotalBytesAreEnough() {
        Stmt2ChunkSizingUtil.BufferSpec current =
                new Stmt2ChunkSizingUtil.BufferSpec(64 * 1024, 16);
        Stmt2ChunkSizingUtil.BufferSpec wanted =
                new Stmt2ChunkSizingUtil.BufferSpec(256 * 1024, 4);

        boolean reusable = Stmt2ChunkSizingUtil.canReuse(
                current, wanted, 800 * 1024L, 4);

        assertFalse(reusable);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deriveWantedSpec_throwsWhenRowsWrittenIsZero() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024, 100, 0);
        Stmt2ChunkSizingUtil.deriveWantedSpec(stats, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deriveWantedSpec_throwsWhenProjectedBytesRequireChunkSizeOverflowingInt() {
        // projectedValueBytes = 3 GB; dynamicMaxChunkBytes needs roundUpToPowerOfTwo(1.5 GB)
        // = 2^31, which exceeds Integer.MAX_VALUE and must not silently truncate to a negative int.
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(3L * 1024 * 1024 * 1024, 100, 1);
        Stmt2ChunkSizingUtil.deriveWantedSpec(stats, 1);
    }

    /**
     * Very large maxSingleValueBytes with modest observedValueBytes must NOT overflow:
     * the dynamicMaxChunkBytes cap kicks in before roundUpToPowerOfTwo, so the result
     * stays well within int range and is a valid positive chunk size.
     */
    @Test
    public void deriveWantedSpec_veryLargeMaxSingleValueIsCappedAndDoesNotOverflow() {
        // 1 MB observed in 1000 rows, scaling to a batch of the same size → projected = 1 MB.
        // maxSingleValueBytes is close to Integer.MAX_VALUE (much larger than projected),
        // but dynamicMaxChunkBytes = roundUpToPowerOfTwo(max(64KB, 512KB)) = 512KB caps it.
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024 * 1024L, Integer.MAX_VALUE / 2, 1000);

        Stmt2ChunkSizingUtil.BufferSpec spec = Stmt2ChunkSizingUtil.deriveWantedSpec(stats, 1000);

        assertTrue("chunkBytes must be positive", spec.getChunkBytes() > 0);
        assertTrue("chunkBytes must fit in int", spec.getChunkBytes() <= Integer.MAX_VALUE);
        assertTrue("reusableChunkCount must be >= 1", spec.getReusableChunkCount() >= 1);
        // chunkBytes is capped at dynamicMaxChunkBytes = 512 KB, not the huge maxSingleValueBytes
        assertEquals(512 * 1024, spec.getChunkBytes());
    }

    // ── BufferSpec constructor validation ────────────────────────────────────

    @Test(expected = IllegalArgumentException.class)
    public void bufferSpec_throwsWhenChunkBytesIsZero() {
        new Stmt2ChunkSizingUtil.BufferSpec(0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferSpec_throwsWhenChunkBytesIsNegative() {
        new Stmt2ChunkSizingUtil.BufferSpec(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferSpec_throwsWhenReusableCountIsZero() {
        new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferSpec_throwsWhenReusableCountIsNegative() {
        new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, -5);
    }

    // ── FieldBatchStats accumulation ─────────────────────────────────────────

    @Test
    public void fieldBatchStats_accumulatesValueBytesAcrossMultipleCalls() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(100 * 1024L, 200, 250);
        stats.recordValueBytes(100 * 1024L, 200, 250);

        assertEquals(200 * 1024L, stats.getObservedValueBytes());
        assertEquals(500, stats.getRowsWritten());
    }

    @Test
    public void fieldBatchStats_tracksMaxSingleValueAcrossMultipleCalls() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(50 * 1024L, 300, 100);
        stats.recordValueBytes(50 * 1024L, 700, 100);
        stats.recordValueBytes(50 * 1024L, 500, 100);

        assertEquals(700, stats.getMaxSingleValueBytes());
    }

    @Test
    public void fieldBatchStats_resetClearsAllAccumulatedState() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);
        stats.setActiveChunksUsed(3);
        stats.setOverflowCount(2);

        stats.reset();

        assertEquals(0L, stats.getObservedValueBytes());
        assertEquals(0, stats.getMaxSingleValueBytes());
        assertEquals(0, stats.getRowsWritten());
        assertEquals(0, stats.getActiveChunksUsed());
        assertEquals(0, stats.getOverflowCount());
    }

    @Test
    public void fieldBatchStats_splitRecordingMatchesSingleCallForProjection() {
        // Two calls of (100KB, maxVal=400, 250 rows) must project identically to
        // a single call of (200KB, maxVal=400, 500 rows).
        Stmt2ChunkSizingUtil.FieldBatchStats split = new Stmt2ChunkSizingUtil.FieldBatchStats();
        split.recordValueBytes(100 * 1024L, 400, 250);
        split.recordValueBytes(100 * 1024L, 400, 250);

        Stmt2ChunkSizingUtil.FieldBatchStats single = new Stmt2ChunkSizingUtil.FieldBatchStats();
        single.recordValueBytes(200 * 1024L, 400, 500);

        assertEquals(
                Stmt2ChunkSizingUtil.projectedValueBytes(single, 2000),
                Stmt2ChunkSizingUtil.projectedValueBytes(split, 2000));
    }



    @Test
    public void projectedValueBytes_scalesObservedBytesToFullBatch() {
        // 200 KB written in 500 rows; scale to 2000 rows → 800 KB
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        long projected = Stmt2ChunkSizingUtil.projectedValueBytes(stats, 2000);

        assertEquals(800 * 1024L, projected);
    }

    @Test
    public void projectedValueBytes_neverShrinksBelowObserved() {
        // batchSizeByRow < rowsWritten → projection < observed → must clamp to observed
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(500 * 1024L, 200, 1000);

        long projected = Stmt2ChunkSizingUtil.projectedValueBytes(stats, 100);

        assertEquals(500 * 1024L, projected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void projectedValueBytes_throwsWhenRowsWrittenIsZero() {
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024, 100, 0);
        Stmt2ChunkSizingUtil.projectedValueBytes(stats, 1000);
    }

    // ── canReuse (stats-aware overload) ─────────────────────────────────────

    @Test
    public void canReuse_statsOverload_acceptsWhenCurrentSpecMeetsWanted() {
        // 200 KB in 500 rows → projected 800 KB for batchSizeByRow 2000.
        // current = 256 KB × 4 = 1 MB reusable; wanted = 256 KB × 4. effectiveActiveChunks = 4 ≤ 8.
        Stmt2ChunkSizingUtil.BufferSpec current = new Stmt2ChunkSizingUtil.BufferSpec(256 * 1024, 4);
        Stmt2ChunkSizingUtil.BufferSpec wanted  = new Stmt2ChunkSizingUtil.BufferSpec(256 * 1024, 4);
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        assertTrue(Stmt2ChunkSizingUtil.canReuse(current, wanted, stats, 2000, 4));
    }

    @Test
    public void canReuse_statsOverload_rejectsWhenCurrentIsUnderprovisioned() {
        // Same projection (800 KB), but current has only 64 KB × 4 = 256 KB reusable vs wanted 1 MB.
        Stmt2ChunkSizingUtil.BufferSpec current = new Stmt2ChunkSizingUtil.BufferSpec(64 * 1024, 4);
        Stmt2ChunkSizingUtil.BufferSpec wanted  = new Stmt2ChunkSizingUtil.BufferSpec(256 * 1024, 4);
        Stmt2ChunkSizingUtil.FieldBatchStats stats = new Stmt2ChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        assertFalse(Stmt2ChunkSizingUtil.canReuse(current, wanted, stats, 2000, 4));
    }

    @Test
    public void shouldShrink_requiresLongUnderuseAndLargeOversize() {
        Stmt2ChunkSizingUtil.BufferSpec current =
                new Stmt2ChunkSizingUtil.BufferSpec(256 * 1024, 4);
        Stmt2ChunkSizingUtil.BufferSpec wanted =
                new Stmt2ChunkSizingUtil.BufferSpec(64 * 1024, 1);

        assertFalse(Stmt2ChunkSizingUtil.shouldShrink(current, wanted, 99));
        assertTrue(Stmt2ChunkSizingUtil.shouldShrink(current, wanted, 100));
    }
}
