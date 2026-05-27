package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import org.junit.Test;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Stmt2VariableWidthReuseHelperTest {

    @Test
    public void createReusableBuffer_bootstrapsAndPrimesCachedChunkCount() {
        Stmt2FieldMeta meta = Stmt2FieldMeta.of(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR,
                (byte) 0);
        WSEWChunkSizingUtil.BufferSpec spec = new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 3);

        Stmt2ColumnFieldBuffer buffer = Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(meta, spec);
        try {
            assertEquals(spec.getChunkBytes(), buffer.currentReusableSpec().getChunkBytes());
            assertEquals(spec.getReusableChunkCount(), buffer.currentReusableSpec().getReusableChunkCount());
            buffer.reset();
            assertEquals(spec.getChunkBytes(), buffer.currentReusableSpec().getChunkBytes());
            assertEquals(spec.getReusableChunkCount(), buffer.currentReusableSpec().getReusableChunkCount());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void resolveBufferSpec_fallsBackToBootstrapSpecForMissingInputs() {
        WSEWChunkSizingUtil.BufferSpec bootstrap = WSEWChunkSizingUtil.bootstrapSpec();

        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(null, 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(new WSEWChunkSizingUtil.BufferSpec[0], 0));
        assertBootstrapSpec(bootstrap,
                Stmt2VariableWidthReuseHelper.resolveBufferSpec(
                        new WSEWChunkSizingUtil.BufferSpec[] {null}, 0));
    }

    @Test
    public void bufferSpecsEqual_comparesChunkBytesAndReusableCount() {
        assertTrue(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 1)));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1),
                new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 2)));
    }

    @Test
    public void bufferSpecsEqual_handlesNullArguments() {
        WSEWChunkSizingUtil.BufferSpec spec = new WSEWChunkSizingUtil.BufferSpec(8 * 1024, 1);

        assertTrue(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, null));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(null, spec));
        assertFalse(Stmt2VariableWidthReuseHelper.bufferSpecsEqual(spec, null));
    }

    private static void assertBootstrapSpec(WSEWChunkSizingUtil.BufferSpec expected,
            WSEWChunkSizingUtil.BufferSpec actual) {
        assertEquals(expected.getChunkBytes(), actual.getChunkBytes());
        assertEquals(expected.getReusableChunkCount(), actual.getReusableChunkCount());
    }
}
