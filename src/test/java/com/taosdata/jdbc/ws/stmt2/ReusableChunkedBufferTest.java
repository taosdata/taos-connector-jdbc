package com.taosdata.jdbc.ws.stmt2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
