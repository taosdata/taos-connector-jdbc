package com.taosdata.jdbc.ws.stmt2;

import java.sql.SQLException;

public final class Stmt2VariableWidthReuseHelper {
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
}
