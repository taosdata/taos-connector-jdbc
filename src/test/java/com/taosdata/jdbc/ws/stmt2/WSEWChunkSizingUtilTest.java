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
