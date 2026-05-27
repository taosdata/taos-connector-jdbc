package com.taosdata.jdbc.ws.stmt2;

import java.sql.SQLException;

public final class Stmt2VariableWidthReuseHelper {
    private static final int MIN_CHUNK_BYTES = 8 * 1024;

    public static final class SizingDecision {
        private final WSEWChunkSizingUtil.BufferSpec nextSpec;
        private final int nextUnderuseStreak;

        public SizingDecision(WSEWChunkSizingUtil.BufferSpec nextSpec, int nextUnderuseStreak) {
            this.nextSpec = nextSpec;
            this.nextUnderuseStreak = nextUnderuseStreak;
        }

        public WSEWChunkSizingUtil.BufferSpec getNextSpec() {
            return nextSpec;
        }

        public int getNextUnderuseStreak() {
            return nextUnderuseStreak;
        }
    }

    private Stmt2VariableWidthReuseHelper() {
    }

    public static WSEWChunkSizingUtil.BufferSpec resolveBufferSpec(
            WSEWChunkSizingUtil.BufferSpec[] specs, int index) {
        if (specs != null && index < specs.length && specs[index] != null) {
            return specs[index];
        }
        return WSEWChunkSizingUtil.bootstrapSpec();
    }

    public static Stmt2ColumnFieldBuffer createReusableVariableWidthBuffer(
            Stmt2FieldMeta meta,
            WSEWChunkSizingUtil.BufferSpec spec) {
        Stmt2ColumnFieldBuffer buffer = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                meta, null, spec.getChunkBytes(), spec.getChunkBytes() / 2, spec.getReusableChunkCount());
        primeReusableBuffer(buffer, spec);
        return buffer;
    }

    public static boolean bufferSpecsEqual(
            WSEWChunkSizingUtil.BufferSpec left,
            WSEWChunkSizingUtil.BufferSpec right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return left.getChunkBytes() == right.getChunkBytes()
                && left.getReusableChunkCount() == right.getReusableChunkCount();
    }

    public static SizingDecision reactiveDecision(
            WSEWChunkSizingUtil.BufferSpec current,
            WSEWChunkSizingUtil.FieldBatchStats stats,
            int underuseStreak) {
        long currentReusableBytes = (long) current.getChunkBytes() * current.getReusableChunkCount();
        boolean shouldGrow = stats.getOverflowCount() > 0
                || stats.getActiveChunksUsed() > 8
                || stats.getObservedValueBytes() > currentReusableBytes;
        if (shouldGrow) {
            long wantedChunkBytes = Math.max(
                    (long) current.getChunkBytes() << 1,
                    roundUpPow2(Math.max(
                            stats.getMaxSingleValueBytes(),
                            stats.getObservedValueBytes() / 4)));
            if (wantedChunkBytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("chunk size overflow: " + wantedChunkBytes);
            }
            int chunkBytes = (int) Math.max(current.getChunkBytes(), wantedChunkBytes);
            int chunkCount = (int) Math.max(
                    1L,
                    (stats.getObservedValueBytes() + chunkBytes - 1) / chunkBytes);
            return new SizingDecision(new WSEWChunkSizingUtil.BufferSpec(chunkBytes, chunkCount), 0);
        }

        if (stats.getObservedValueBytes() < currentReusableBytes / 2) {
            if (underuseStreak >= WSEWChunkSizingUtil.SHRINK_STREAK_THRESHOLD) {
                if (current.getReusableChunkCount() > 1) {
                    return new SizingDecision(
                            new WSEWChunkSizingUtil.BufferSpec(
                                    current.getChunkBytes(),
                                    current.getReusableChunkCount() - 1),
                            0);
                }
                int smallerChunk = Math.max(MIN_CHUNK_BYTES, current.getChunkBytes() >> 1);
                return new SizingDecision(new WSEWChunkSizingUtil.BufferSpec(smallerChunk, 1), 0);
            }
            return new SizingDecision(current, underuseStreak + 1);
        }

        return new SizingDecision(current, 0);
    }

    private static void primeReusableBuffer(
            Stmt2ColumnFieldBuffer buffer,
            WSEWChunkSizingUtil.BufferSpec spec) {
        byte[] chunk = new byte[spec.getChunkBytes()];
        try {
            for (int i = 0; i < spec.getReusableChunkCount(); i++) {
                buffer.appendBytes(chunk);
            }
            buffer.reset();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to prime reusable stmt2 buffer", e);
        }
    }

    private static int roundUpPow2(long value) {
        long adjusted = Math.max(1L, value);
        long highest = Long.highestOneBit(adjusted);
        long result = highest == adjusted ? highest : highest << 1;
        if (result <= 0 || result > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("chunk size overflow: " + value);
        }
        return (int) Math.max(MIN_CHUNK_BYTES, result);
    }
}
