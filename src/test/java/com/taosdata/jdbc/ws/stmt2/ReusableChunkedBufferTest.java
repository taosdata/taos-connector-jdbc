package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ReusableChunkedBufferTest {

    @Test
    public void writeBytesAndReset_reusesStandardChunks() throws Exception {
        Object buffer = newBuffer(8, 6, 4);
        try {
            writeBytes(buffer, "abcde");
            writeBytes(buffer, "fghij");

            assertEquals("abcdefghij", dumpUtf8(buffer));
            List<?> cachedBeforeReset = cachedStandardChunks(buffer);
            assertEquals(2, cachedBeforeReset.size());
            Object firstChunk = cachedBeforeReset.get(0);
            Object secondChunk = cachedBeforeReset.get(1);

            invoke(buffer, "reset", new Class<?>[0]);
            writeBytes(buffer, "xy");

            assertEquals("xy", dumpUtf8(buffer));
            List<?> cachedAfterReset = cachedStandardChunks(buffer);
            assertEquals(2, cachedAfterReset.size());
            assertSame(firstChunk, cachedAfterReset.get(0));
            assertSame(secondChunk, cachedAfterReset.get(1));
        } finally {
            release(buffer);
        }
    }

    @Test
    public void reusableChunkedBuffer_exposesChunkSizingState() throws Exception {
        Class<?> clazz = Class.forName("com.taosdata.jdbc.ws.stmt2.ReusableChunkedBuffer");
        Constructor<?> ctor = clazz.getDeclaredConstructor(int.class, int.class, int.class);
        ctor.setAccessible(true);
        Object buffer = ctor.newInstance(8 * 1024, 4 * 1024, 3);

        Method writeBytes = clazz.getDeclaredMethod("writeBytes", byte[].class, int.class, int.class);
        writeBytes.setAccessible(true);
        writeBytes.invoke(buffer, new byte[10 * 1024], 0, 10 * 1024);

        Method chunkBytes = clazz.getDeclaredMethod("chunkBytes");
        Method cachedChunkCount = clazz.getDeclaredMethod("cachedReusableChunkCount");
        Method activeChunkCount = clazz.getDeclaredMethod("activeChunkCount");
        chunkBytes.setAccessible(true);
        cachedChunkCount.setAccessible(true);
        activeChunkCount.setAccessible(true);

        assertEquals(8 * 1024, chunkBytes.invoke(buffer));
        assertEquals(2, activeChunkCount.invoke(buffer));
        assertEquals(2, cachedChunkCount.invoke(buffer));
    }

    @Test
    public void reusableChunkedBuffer_tracksOverflowWhenBatchNeedsNonReusableChunk() throws Exception {
        Class<?> clazz = Class.forName("com.taosdata.jdbc.ws.stmt2.ReusableChunkedBuffer");
        Constructor<?> ctor = clazz.getDeclaredConstructor(int.class, int.class, int.class);
        ctor.setAccessible(true);
        Object buffer = ctor.newInstance(8 * 1024, 4 * 1024, 1);

        Method writeBytes = clazz.getDeclaredMethod("writeBytes", byte[].class, int.class, int.class);
        Method overflowCount = clazz.getDeclaredMethod("overflowCount");
        Method reset = clazz.getDeclaredMethod("reset");
        writeBytes.setAccessible(true);
        overflowCount.setAccessible(true);
        reset.setAccessible(true);

        writeBytes.invoke(buffer, new byte[20 * 1024], 0, 20 * 1024);

        assertEquals(2, overflowCount.invoke(buffer));

        reset.invoke(buffer);
        assertEquals(0, overflowCount.invoke(buffer));
    }

    @Test
    public void columnFieldBuffer_exposesReusableOverflowCount() throws Exception {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        Stmt2ColumnFieldBuffer buffer = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                meta, null, 8 * 1024, 4 * 1024, 1);
        try {
            buffer.appendString(String.join("", Collections.nCopies(20_000, "a")));
            assertEquals(1, buffer.reusableOverflowCount());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void writeString_oversized_incrementsOverflowCount() throws Exception {
        Object buffer = newBuffer(8, 4, 2);
        try {
            assertEquals(10, invokeWriteString(buffer, "1234567890"));
            assertEquals(1, overflowCount(buffer));
        } finally {
            release(buffer);
        }
    }

    @Test
    public void reset_releasesDedicatedChunks() throws Exception {
        Object buffer = newBuffer(8, 6, 2);
        try {
            writeBytes(buffer, "abc");
            writeBytes(buffer, "123456");  // 6 bytes — in dedicated range [dedicatedThreshold=6, standardChunkBytes=8)

            Object dedicatedChunk = activeChunkBuffer(buffer, 1);
            assertTrue(((ByteBuf) dedicatedChunk).refCnt() > 0);

            invoke(buffer, "reset", new Class<?>[0]);

            assertEquals(0, ((ByteBuf) dedicatedChunk).refCnt());
            assertEquals(0, activeChunks(buffer).size());
            assertEquals(1, cachedStandardChunks(buffer).size());
        } finally {
            release(buffer);
        }
    }

    @Test
    public void writeString_oversized_routesToDedicatedChunk_noCacheInflation() throws Exception {
        // standardChunkBytes=8, dedicatedThreshold=4, maxReusable=2
        // Use writeBytes (not writeString) for the initial data so the first standard
        // chunk is actually cached (writeBytes always uses the standard-chunk path).
        Object buffer = newBuffer(8, 4, 2);
        try {
            // Write 2 bytes → goes to a new standard chunk (cached)
            writeBytes(buffer, "ab");
            assertEquals(1, cachedStandardChunks(buffer).size());
            ByteBuf cachedChunk = (ByteBuf) cachedStandardChunks(buffer).get(0);
            int originalCapacity = cachedChunk.capacity();

            // Write an oversized string: 10 bytes > standardChunkBytes=8
            invokeWriteString(buffer, "1234567890");

            // Oversized write must not have added a second entry to the standard cache
            assertEquals(1, cachedStandardChunks(buffer).size());

            // The cached standard chunk must NOT have been expanded
            assertEquals("cached chunk must not be inflated", originalCapacity, cachedChunk.capacity());

            // There should be 2 active chunks: the standard one and the oversized dedicated one
            assertEquals(2, activeChunks(buffer).size());

            // Content is intact
            assertEquals("ab1234567890", dumpUtf8(buffer));

            // After reset, cached chunk count and capacity are unchanged
            invoke(buffer, "reset", new Class<?>[0]);
            assertEquals(1, cachedStandardChunks(buffer).size());
            ByteBuf cachedAfterReset = (ByteBuf) cachedStandardChunks(buffer).get(0);
            assertEquals("cached chunk must not be inflated after reset",
                    originalCapacity, cachedAfterReset.capacity());
        } finally {
            release(buffer);
        }
    }

    @Test
    public void writeString_fitsInStandardChunk_usesReusablePath() throws Exception {
        // Verify strings that fit within standardChunkBytes take the reusable standard-chunk path
        // standardChunkBytes=16, dedicatedThreshold=12, maxReusable=2
        Object buffer = newBuffer(16, 12, 2);
        try {
            // 8-byte string: fits in standard chunk (8 < 16) → uses reusable path
            invokeWriteString(buffer, "abcdefgh");

            // Write landed in the standard (reusable) chunk cache
            assertEquals(1, cachedStandardChunks(buffer).size());
            assertEquals(1, activeChunks(buffer).size());

            // The active chunk IS the cached standard chunk — identity check proves reusable path
            Object cachedChunk = cachedStandardChunks(buffer).get(0);
            assertSame("active chunk must be the cached standard chunk (reusable path)",
                    cachedChunk, activeChunkBuffer(buffer, 0));

            // 16-byte string (= standardChunkBytes): still reusable path (not > standardChunkBytes)
            invokeWriteString(buffer, "0123456789abcdef");
            assertEquals("abcdefgh0123456789abcdef", dumpUtf8(buffer));
            // Standard chunk cache must remain unpolluted
            assertEquals(1, cachedStandardChunks(buffer).size());

            // Reset: standard chunks must be KEPT (not released), ready for reuse
            invoke(buffer, "reset", new Class<?>[0]);
            assertEquals("standard chunk must survive reset", 1, cachedStandardChunks(buffer).size());

            // Re-write after reset must reuse the SAME chunk instance (not allocate a new one)
            invokeWriteString(buffer, "abcdefgh");
            assertSame("standard chunk must be reused after reset (identity check)",
                    cachedChunk, activeChunkBuffer(buffer, 0));
        } finally {
            release(buffer);
        }
    }

    private static int invokeWriteString(Object buffer, String value) throws Exception {
        return (int) invoke(buffer, "writeString", new Class<?>[]{String.class}, value);
    }

    private static Object newBuffer(int standardChunkBytes, int dedicatedThresholdBytes, int maxReusableChunks) throws Exception {
        Class<?> clazz = Class.forName("com.taosdata.jdbc.ws.stmt2.ReusableChunkedBuffer");
        Constructor<?> ctor = clazz.getDeclaredConstructor(int.class, int.class, int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(standardChunkBytes, dedicatedThresholdBytes, maxReusableChunks);
    }

    private static void writeBytes(Object buffer, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        invoke(buffer, "writeBytes", new Class<?>[]{byte[].class, int.class, int.class}, bytes, 0, bytes.length);
    }

    private static String dumpUtf8(Object buffer) throws Exception {
        CompositeByteBuf out = PooledByteBufAllocator.DEFAULT.compositeBuffer(16);
        try {
            invoke(buffer, "appendReadableComponents", new Class<?>[]{CompositeByteBuf.class}, out);
            return new String(ByteBufUtil.getBytes(out), StandardCharsets.UTF_8);
        } finally {
            out.release();
        }
    }

    private static List<?> cachedStandardChunks(Object buffer) throws Exception {
        return (List<?>) getDeclaredField(buffer, "cachedStandardChunks");
    }

    private static List<?> activeChunks(Object buffer) throws Exception {
        return (List<?>) getDeclaredField(buffer, "activeChunks");
    }

    private static int overflowCount(Object buffer) throws Exception {
        return (int) invoke(buffer, "overflowCount", new Class<?>[0]);
    }

    private static Object activeChunkBuffer(Object buffer, int index) throws Exception {
        Object chunkRef = activeChunks(buffer).get(index);
        return getDeclaredField(chunkRef, "buf");
    }

    private static void release(Object buffer) throws Exception {
        invoke(buffer, "release", new Class<?>[0]);
    }

    private static Object invoke(Object target, String method, Class<?>[] types, Object... args) throws Exception {
        Method m = target.getClass().getDeclaredMethod(method, types);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    private static Object getDeclaredField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
