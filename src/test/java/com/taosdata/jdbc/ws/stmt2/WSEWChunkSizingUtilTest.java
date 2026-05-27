package com.taosdata.jdbc.ws.stmt2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WSEWChunkSizingUtilTest {

    @Test
    public void bootstrapSpec_defaultsTo8KbSingleChunk() {
        WSEWChunkSizingUtil.BufferSpec spec = WSEWChunkSizingUtil.bootstrapSpec();

        assertEquals(8 * 1024, spec.getChunkBytes());
        assertEquals(1, spec.getReusableChunkCount());
    }

    @Test
    public void deriveWantedSpec_projectsObservedBytesToFullBatch() {
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024, 400, 500);

        WSEWChunkSizingUtil.BufferSpec spec =
                WSEWChunkSizingUtil.deriveWantedSpec(stats, 2000);

        assertEquals(256 * 1024, spec.getChunkBytes());
        assertEquals(4, spec.getReusableChunkCount());
    }

    @Test
    public void canReuse_rejectsSmallChunksEvenWhenTotalBytesAreEnough() {
        WSEWChunkSizingUtil.BufferSpec current =
                new WSEWChunkSizingUtil.BufferSpec(64 * 1024, 16);
        WSEWChunkSizingUtil.BufferSpec wanted =
                new WSEWChunkSizingUtil.BufferSpec(256 * 1024, 4);

        boolean reusable = WSEWChunkSizingUtil.canReuse(
                current, wanted, 800 * 1024L, 4);

        assertFalse(reusable);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deriveWantedSpec_throwsWhenRowsWrittenIsZero() {
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024, 100, 0);
        WSEWChunkSizingUtil.deriveWantedSpec(stats, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deriveWantedSpec_throwsWhenProjectedBytesRequireChunkSizeOverflowingInt() {
        // projectedValueBytes = 3 GB; dynamicMaxChunkBytes needs roundUpToPowerOfTwo(1.5 GB)
        // = 2^31, which exceeds Integer.MAX_VALUE and must not silently truncate to a negative int.
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(3L * 1024 * 1024 * 1024, 100, 1);
        WSEWChunkSizingUtil.deriveWantedSpec(stats, 1);
    }

    /**
     * Very large maxSingleValueBytes with modest observedValueBytes must NOT overflow:
     * the dynamicMaxChunkBytes cap kicks in before roundUpToPowerOfTwo, so the result
     * stays well within int range and is a valid positive chunk size.
     */
    @Test
    public void deriveWantedSpec_veryLargeMaxSingleValueIsCappeAndDoesNotOverflow() {
        // 1 MB observed in 1000 rows, scaling to a batch of the same size → projected = 1 MB.
        // maxSingleValueBytes is close to Integer.MAX_VALUE (much larger than projected),
        // but dynamicMaxChunkBytes = roundUpToPowerOfTwo(max(64KB, 512KB)) = 512KB caps it.
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024 * 1024L, Integer.MAX_VALUE / 2, 1000);

        WSEWChunkSizingUtil.BufferSpec spec = WSEWChunkSizingUtil.deriveWantedSpec(stats, 1000);

        assertTrue("chunkBytes must be positive", spec.getChunkBytes() > 0);
        assertTrue("chunkBytes must fit in int", spec.getChunkBytes() <= Integer.MAX_VALUE);
        assertTrue("reusableChunkCount must be >= 1", spec.getReusableChunkCount() >= 1);
        // chunkBytes is capped at dynamicMaxChunkBytes = 512 KB, not the huge maxSingleValueBytes
        assertEquals(512 * 1024, spec.getChunkBytes());
    }

    // ── projectedValueBytes ──────────────────────────────────────────────────

    @Test
    public void projectedValueBytes_scalesObservedBytesToFullBatch() {
        // 200 KB written in 500 rows; scale to 2000 rows → 800 KB
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        long projected = WSEWChunkSizingUtil.projectedValueBytes(stats, 2000);

        assertEquals(800 * 1024L, projected);
    }

    @Test
    public void projectedValueBytes_neverShrinksBelowObserved() {
        // batchSizeByRow < rowsWritten → projection < observed → must clamp to observed
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(500 * 1024L, 200, 1000);

        long projected = WSEWChunkSizingUtil.projectedValueBytes(stats, 100);

        assertEquals(500 * 1024L, projected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void projectedValueBytes_throwsWhenRowsWrittenIsZero() {
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(1024, 100, 0);
        WSEWChunkSizingUtil.projectedValueBytes(stats, 1000);
    }

    // ── canReuse (stats-aware overload) ─────────────────────────────────────

    @Test
    public void canReuse_statsOverload_acceptsWhenCurrentSpecMeetsWanted() {
        // 200 KB in 500 rows → projected 800 KB for batchSizeByRow 2000.
        // current = 256 KB × 4 = 1 MB reusable; wanted = 256 KB × 4. effectiveActiveChunks = 4 ≤ 8.
        WSEWChunkSizingUtil.BufferSpec current = new WSEWChunkSizingUtil.BufferSpec(256 * 1024, 4);
        WSEWChunkSizingUtil.BufferSpec wanted  = new WSEWChunkSizingUtil.BufferSpec(256 * 1024, 4);
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        assertTrue(WSEWChunkSizingUtil.canReuse(current, wanted, stats, 2000, 4));
    }

    @Test
    public void canReuse_statsOverload_rejectsWhenCurrentIsUnderprovisioned() {
        // Same projection (800 KB), but current has only 64 KB × 4 = 256 KB reusable vs wanted 1 MB.
        WSEWChunkSizingUtil.BufferSpec current = new WSEWChunkSizingUtil.BufferSpec(64 * 1024, 4);
        WSEWChunkSizingUtil.BufferSpec wanted  = new WSEWChunkSizingUtil.BufferSpec(256 * 1024, 4);
        WSEWChunkSizingUtil.FieldBatchStats stats = new WSEWChunkSizingUtil.FieldBatchStats();
        stats.recordValueBytes(200 * 1024L, 400, 500);

        assertFalse(WSEWChunkSizingUtil.canReuse(current, wanted, stats, 2000, 4));
    }

    @Test
    public void shouldShrink_requiresLongUnderuseAndLargeOversize() {
        WSEWChunkSizingUtil.BufferSpec current =
                new WSEWChunkSizingUtil.BufferSpec(256 * 1024, 4);
        WSEWChunkSizingUtil.BufferSpec wanted =
                new WSEWChunkSizingUtil.BufferSpec(64 * 1024, 1);

        assertFalse(WSEWChunkSizingUtil.shouldShrink(current, wanted, 99));
        assertTrue(WSEWChunkSizingUtil.shouldShrink(current, wanted, 100));
    }
}
