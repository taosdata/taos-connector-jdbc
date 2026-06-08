package com.taosdata.jdbc.ws.stmt2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FixedWidthRawSlabTest {

    @Test
    public void appendFixed4AndNull_exposesLittleEndianValueAndNullSlices() throws Exception {
        Object slab = newSlab(4, 2);

        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 0x01020304);
        invoke(slab, "appendNull", new Class<?>[0]);
        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 0x0A0B0C0D);

        assertEquals(3, invokeInt(slab, "rowCount"));
        assertArrayEquals(new byte[]{0, 1, 0}, sliceBytes(slab, "nullSlice"));
        assertArrayEquals(
                new byte[]{
                        0x04, 0x03, 0x02, 0x01,
                        0x00, 0x00, 0x00, 0x00,
                        0x0D, 0x0C, 0x0B, 0x0A
                },
                sliceBytes(slab, "valueSlice"));
    }

    @Test
    public void reset_keepsCapacityAndReusesPrefixOnly() throws Exception {
        Object slab = newSlab(4, 1);

        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 1);
        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 2);
        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 3);

        int grownCapacity = invokeInt(slab, "rowCapacity");
        assertTrue(grownCapacity >= 3);

        invoke(slab, "reset", new Class<?>[0]);
        invoke(slab, "appendFixed4", new Class<?>[]{int.class}, 99);

        assertEquals(1, invokeInt(slab, "rowCount"));
        assertEquals(grownCapacity, invokeInt(slab, "rowCapacity"));
        assertArrayEquals(new byte[]{0}, sliceBytes(slab, "nullSlice"));
        assertArrayEquals(new byte[]{99, 0, 0, 0}, sliceBytes(slab, "valueSlice"));
    }

    @Test
    public void appendFixed1_respectsRowStride() throws Exception {
        Object slab = newSlab(4, 2);

        invoke(slab, "appendFixed1", new Class<?>[]{byte.class}, (byte) 7);
        invoke(slab, "appendFixed1", new Class<?>[]{byte.class}, (byte) 9);

        assertArrayEquals(
                new byte[]{
                        7, 0, 0, 0,
                        9, 0, 0, 0
                },
                sliceBytes(slab, "valueSlice"));
    }

    private static Object newSlab(int fixedWidth, int initialRows) throws Exception {
        Class<?> clazz = Class.forName("com.taosdata.jdbc.ws.stmt2.FixedWidthRawSlab");
        Constructor<?> constructor = clazz.getDeclaredConstructor(int.class, int.class);
        constructor.setAccessible(true);
        return constructor.newInstance(fixedWidth, initialRows);
    }

    private static Object invoke(Object target, String method, Class<?>[] types, Object... args) throws Exception {
        Method m = target.getClass().getDeclaredMethod(method, types);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    private static int invokeInt(Object target, String method) throws Exception {
        return (Integer) invoke(target, method, new Class<?>[0]);
    }

    private static byte[] sliceBytes(Object slab, String method) throws Exception {
        ByteBuf buf = (ByteBuf) invoke(slab, method, new Class<?>[0]);
        try {
            return ByteBufUtil.getBytes(buf);
        } finally {
            buf.release();
        }
    }
}
