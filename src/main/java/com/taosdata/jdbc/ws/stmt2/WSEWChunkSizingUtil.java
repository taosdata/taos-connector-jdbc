package com.taosdata.jdbc.ws.stmt2;

public final class WSEWChunkSizingUtil {
    private static final int BOOTSTRAP_CHUNK_BYTES = 8 * 1024;
    private static final int TARGET_ACTIVE_CHUNKS = 4;
    private static final int MIN_ACTIVE_CHUNKS = 2;
    private static final int SHRINK_STREAK_THRESHOLD = 100;

    public static final class BufferSpec {
        private final int chunkBytes;
        private final int reusableChunkCount;

        public BufferSpec(int chunkBytes, int reusableChunkCount) {
            if (chunkBytes <= 0) {
                throw new IllegalArgumentException("chunkBytes must be > 0; got " + chunkBytes);
            }
            if (reusableChunkCount <= 0) {
                throw new IllegalArgumentException("reusableChunkCount must be > 0; got " + reusableChunkCount);
            }
            this.chunkBytes = chunkBytes;
            this.reusableChunkCount = reusableChunkCount;
        }

        public int getChunkBytes() {
            return chunkBytes;
        }

        public int getReusableChunkCount() {
            return reusableChunkCount;
        }
    }

    public static final class FieldBatchStats {
        private int rowsWritten;
        private long observedValueBytes;
        private int maxSingleValueBytes;
        private int activeChunksUsed;
        private int overflowCount;

        /**
         * Accumulates stats from one or more rows written to a field buffer. Safe to call
         * repeatedly; each invocation adds to the running totals so write-time instrumentation
         * can record each append without re-scanning.
         *
         * <p>To start a fresh batch, create a new instance or call {@link #reset()}.
         *
         * @param valueBytes        bytes contributed by this group of rows
         * @param maxSingleValueBytes largest single-value size observed in this group
         * @param rowsWritten       number of rows in this group (must be &ge; 0)
         */
        public void recordValueBytes(long valueBytes, int maxSingleValueBytes, int rowsWritten) {
            this.observedValueBytes += valueBytes;
            if (maxSingleValueBytes > this.maxSingleValueBytes) {
                this.maxSingleValueBytes = maxSingleValueBytes;
            }
            this.rowsWritten += rowsWritten;
        }

        /** Resets all accumulated state so this instance can be reused for the next batch. */
        public void reset() {
            this.observedValueBytes = 0;
            this.maxSingleValueBytes = 0;
            this.rowsWritten = 0;
            this.activeChunksUsed = 0;
            this.overflowCount = 0;
        }

        public long getObservedValueBytes() {
            return observedValueBytes;
        }

        public int getMaxSingleValueBytes() {
            return maxSingleValueBytes;
        }

        public int getRowsWritten() {
            return rowsWritten;
        }

        public int getActiveChunksUsed() {
            return activeChunksUsed;
        }

        public void setActiveChunksUsed(int activeChunksUsed) {
            this.activeChunksUsed = activeChunksUsed;
        }

        public int getOverflowCount() {
            return overflowCount;
        }

        public void setOverflowCount(int overflowCount) {
            this.overflowCount = overflowCount;
        }
    }

    public static BufferSpec bootstrapSpec() {
        return new BufferSpec(BOOTSTRAP_CHUNK_BYTES, 1);
    }

    /**
     * Returns the projected total value bytes for a full batch of {@code batchSizeByRow} rows,
     * scaled from the observed bytes in {@code stats}. The result is at least the already-observed
     * bytes so it never shrinks below what was actually written.
     *
     * <p>This is the single authoritative formula used by both {@link #deriveWantedSpec} and the
     * stats-aware {@link #canReuse} overload; callers should prefer these entry points rather than
     * replicating the scaling arithmetic.
     */
    public static long projectedValueBytes(FieldBatchStats stats, int batchSizeByRow) {
        if (stats.getRowsWritten() == 0) {
            throw new IllegalArgumentException(
                    "stats.rowsWritten must be > 0; got 0, which would cause division by zero");
        }
        return Math.max(
                stats.getObservedValueBytes(),
                (long) Math.ceil((double) stats.getObservedValueBytes() * batchSizeByRow / stats.getRowsWritten()));
    }

    public static BufferSpec deriveWantedSpec(FieldBatchStats stats, int batchSizeByRow) {
        long projected = projectedValueBytes(stats, batchSizeByRow);
        long perChunkTarget = Math.max(1L, projected / TARGET_ACTIVE_CHUNKS);
        long chunkCandidate = Math.max(stats.getMaxSingleValueBytes(), perChunkTarget);
        // dynamicMaxChunkBytes is the cap; it is a valid int power-of-two or throws.
        long dynamicMaxChunkBytes = roundUpToPowerOfTwo(Math.max(64L * 1024, projected / MIN_ACTIVE_CHUNKS));
        // Cap chunkCandidate before rounding so roundUpToPowerOfTwo cannot exceed the already-validated cap.
        long chunkCandidateCapped = Math.min(chunkCandidate, dynamicMaxChunkBytes);
        int wantedChunkBytes = (int) Math.max(BOOTSTRAP_CHUNK_BYTES,
                Math.min(dynamicMaxChunkBytes, roundUpToPowerOfTwo(chunkCandidateCapped)));
        int wantedChunkCount = (int) Math.max(1L,
                (projected + wantedChunkBytes - 1) / wantedChunkBytes);
        return new BufferSpec(wantedChunkBytes, wantedChunkCount);
    }

    /**
     * Returns whether {@code current} is reusable given a raw pre-computed {@code projectedValueBytes}.
     * Prefer the stats-aware overload when the projection has not already been computed.
     */
    public static boolean canReuse(BufferSpec current, BufferSpec wanted, long projectedValueBytes, int targetActiveChunks) {
        long currentReusableBytes = (long) current.getChunkBytes() * current.getReusableChunkCount();
        long wantedReusableBytes = (long) wanted.getChunkBytes() * wanted.getReusableChunkCount();
        long effectiveActiveChunks = (projectedValueBytes + current.getChunkBytes() - 1) / current.getChunkBytes();
        return currentReusableBytes >= wantedReusableBytes && effectiveActiveChunks <= targetActiveChunks * 2L;
    }

    /**
     * Stats-aware overload: computes the projection internally so callers do not need to replicate
     * the scaling formula. Equivalent to {@code canReuse(current, wanted, projectedValueBytes(stats,
     * batchSizeByRow), targetActiveChunks)}.
     */
    public static boolean canReuse(BufferSpec current, BufferSpec wanted,
            FieldBatchStats stats, int batchSizeByRow, int targetActiveChunks) {
        return canReuse(current, wanted, projectedValueBytes(stats, batchSizeByRow), targetActiveChunks);
    }

    public static boolean shouldShrink(BufferSpec current, BufferSpec wanted, int underuseStreak) {
        long currentReusableBytes = (long) current.getChunkBytes() * current.getReusableChunkCount();
        long wantedReusableBytes = (long) wanted.getChunkBytes() * wanted.getReusableChunkCount();
        return underuseStreak >= SHRINK_STREAK_THRESHOLD && currentReusableBytes >= wantedReusableBytes * 2L;
    }

    private static int roundUpToPowerOfTwo(long value) {
        long adjusted = Math.max(1L, value);
        long highest = Long.highestOneBit(adjusted);
        long result = (highest == adjusted) ? highest : (highest << 1);
        if (result <= 0 || result > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Chunk size candidate " + value + " rounds up to a value that overflows int ("
                    + result + "); reduce batchSizeByRow or observed bytes");
        }
        return (int) result;
    }

    private WSEWChunkSizingUtil() {
    }
}
