package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.IdentityHashMap;

final class ReusableChunkedBuffer {
    private static final class ChunkRef {
        private final ByteBuf buf;
        private final boolean reusable;

        private ChunkRef(ByteBuf buf, boolean reusable) {
            this.buf = buf;
            this.reusable = reusable;
        }
    }

    private final PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private final int standardChunkBytes;
    private final int dedicatedThresholdBytes;
    private final int maxReusableChunks;
    private final ArrayList<ByteBuf> cachedStandardChunks = new ArrayList<>();
    private final ArrayList<ChunkRef> activeChunks = new ArrayList<>();
    private int nextReusableIndex = 0;
    private ByteBuf currentChunk;
    private int totalBytes = 0;
    private int overflowCount = 0;

    ReusableChunkedBuffer(int standardChunkBytes, int dedicatedThresholdBytes, int maxReusableChunks) {
        if (standardChunkBytes <= 0) {
            throw new IllegalArgumentException("standardChunkBytes must be > 0");
        }
        if (maxReusableChunks <= 0) {
            throw new IllegalArgumentException("maxReusableChunks must be > 0");
        }
        this.standardChunkBytes = standardChunkBytes;
        this.dedicatedThresholdBytes = Math.max(1, Math.min(dedicatedThresholdBytes, standardChunkBytes));
        this.maxReusableChunks = maxReusableChunks;
    }

    int writeBytes(byte[] src, int off, int len) {
        if (len <= 0) {
            return 0;
        }
        int offset = off;
        int remaining = len;
        while (remaining > 0) {
            int batchSize = Math.min(remaining, standardChunkBytes);
            ByteBuf target = acquireChunk(batchSize);
            int toWrite = Math.min(remaining, target.writableBytes());
            target.writeBytes(src, offset, toWrite);
            remaining -= toWrite;
            offset += toWrite;
        }
        totalBytes += len;
        return len;
    }

    int writeString(String value) {
        return writeString(value, ByteBufUtil.utf8Bytes(value));
    }

    int writeString(String value, int utf8Length) {
        assert value == null || utf8Length == ByteBufUtil.utf8Bytes(value);
        if (utf8Length <= 0) {
            return 0;
        }
        if (utf8Length > standardChunkBytes) {
            // Oversized string: bypass the standard-chunk pool to avoid auto-expanding
            // (and permanently inflating) a cached reusable chunk.
            overflowCount++;
            ByteBuf dedicated = allocator.buffer(utf8Length);
            boolean success = false;
            try {
                int writtenUtf8Length = ByteBufUtil.writeUtf8(dedicated, value);
                activeChunks.add(new ChunkRef(dedicated, false));
                currentChunk = dedicated;
                totalBytes += writtenUtf8Length;
                success = true;
                return writtenUtf8Length;
            } finally {
                if (!success) {
                    Utils.releaseByteBuf(dedicated);
                }
            }
        }
        ByteBuf target = acquireChunk(utf8Length);
        int writtenUtf8Length = ByteBufUtil.writeUtf8(target, value);
        totalBytes += writtenUtf8Length;
        return writtenUtf8Length;
    }

    int chunkBytes() {
        return standardChunkBytes;
    }

    int cachedReusableChunkCount() {
        return cachedStandardChunks.size();
    }

    int activeChunkCount() {
        return activeChunks.size();
    }

    int activeReusableChunkCount() {
        int count = 0;
        for (ChunkRef ref : activeChunks) {
            if (ref.reusable) {
                count++;
            }
        }
        return count;
    }

    int overflowCount() {
        return overflowCount;
    }

    int readableBytes() {
        return totalBytes;
    }

    int componentCount() {
        int count = 0;
        for (ChunkRef ref : activeChunks) {
            if (ref.buf.readableBytes() > 0) {
                count++;
            }
        }
        return count;
    }

    int cachedCapacityBytes() {
        int total = 0;
        for (ByteBuf chunk : cachedStandardChunks) {
            total += chunk.capacity();
        }
        return total;
    }

    void appendReadableComponents(CompositeByteBuf target) {
        for (ChunkRef ref : activeChunks) {
            int readableBytes = ref.buf.readableBytes();
            if (readableBytes > 0) {
                ByteBuf slice = ref.buf.retainedSlice(ref.buf.readerIndex(), readableBytes);
                boolean success = false;
                try {
                    target.addComponent(true, slice);
                    success = true;
                } finally {
                    if (!success) {
                        Utils.releaseByteBuf(slice);
                    }
                }
            }
        }
    }

    void reset() {
        for (ChunkRef ref : activeChunks) {
            if (ref.reusable) {
                ref.buf.clear();
            } else {
                Utils.releaseByteBuf(ref.buf);
            }
        }
        activeChunks.clear();
        nextReusableIndex = 0;
        currentChunk = null;
        totalBytes = 0;
        overflowCount = 0;
    }

    void release() {
        IdentityHashMap<ByteBuf, Boolean> released = new IdentityHashMap<>();
        for (ChunkRef ref : activeChunks) {
            if (released.put(ref.buf, Boolean.TRUE) == null) {
                Utils.releaseByteBuf(ref.buf);
            }
        }
        activeChunks.clear();
        for (ByteBuf chunk : cachedStandardChunks) {
            if (released.put(chunk, Boolean.TRUE) == null) {
                Utils.releaseByteBuf(chunk);
            }
        }
        cachedStandardChunks.clear();
        nextReusableIndex = 0;
        currentChunk = null;
        totalBytes = 0;
    }

    void primeReusableChunks(int reusableChunkCount) {
        for (int i = 0; i < reusableChunkCount; i++) {
            ByteBuf chunk = acquireChunk(standardChunkBytes);
            chunk.writerIndex(standardChunkBytes);
            totalBytes += standardChunkBytes;
        }
        reset();
    }

    private ByteBuf acquireChunk(int minWritableBytes) {
        if (currentChunk != null && currentChunk.writableBytes() >= minWritableBytes) {
            return currentChunk;
        }

        if (minWritableBytes >= dedicatedThresholdBytes && minWritableBytes < standardChunkBytes) {
            overflowCount++;
            ByteBuf chunk = allocator.buffer(minWritableBytes);
            activeChunks.add(new ChunkRef(chunk, false));
            currentChunk = chunk;
            return chunk;
        }

        if (nextReusableIndex < cachedStandardChunks.size()) {
            ByteBuf chunk = cachedStandardChunks.get(nextReusableIndex++);
            chunk.clear();
            activeChunks.add(new ChunkRef(chunk, true));
            currentChunk = chunk;
            return chunk;
        }

        ByteBuf chunk = allocator.buffer(standardChunkBytes);
        boolean reusable = cachedStandardChunks.size() < maxReusableChunks;
        if (reusable) {
            cachedStandardChunks.add(chunk);
            nextReusableIndex++;
        } else {
            overflowCount++;
        }
        activeChunks.add(new ChunkRef(chunk, reusable));
        currentChunk = chunk;
        return chunk;
    }
}
