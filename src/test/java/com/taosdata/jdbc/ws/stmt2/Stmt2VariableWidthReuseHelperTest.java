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
    public void bufferSpecsEqual_matchesChunkBytesAndReusableCount() {
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
}
