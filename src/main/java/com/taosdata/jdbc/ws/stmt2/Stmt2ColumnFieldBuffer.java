package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.common.AutoExpandingBuffer;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * Append-only accumulator for a single stmt2 column field.
 *
 * <p>One buffer is kept per prepared-statement field. Callers append row values
 * one at a time; the buffer then exposes the per-column block layout as a
 * composite ByteBuf so execute-time payload assembly can reuse the accumulated
 * per-column buffers instead of flattening them through {@code byte[]}.
 */
public final class Stmt2ColumnFieldBuffer {
    private static final int MAX_COMPONENT_COUNT = 4096;
    private static final int NULL_BUFFER_INIT_SIZE = 1024;
    private static final int LENGTH_BUFFER_INIT_SIZE = 4096;
    private static final int VALUE_BUFFER_INIT_SIZE = 8192;

    public static final class BufferSizeHints {
        private final int nullBytes;
        private final int lengthBytes;
        private final int valueBytes;

        public BufferSizeHints(int nullBytes, int lengthBytes, int valueBytes) {
            this.nullBytes = nullBytes;
            this.lengthBytes = lengthBytes;
            this.valueBytes = valueBytes;
        }
    }

    private final Stmt2FieldMeta meta;
    private final FixedWidthRawSlab fixedWidthSlab;
    private final AutoExpandingBuffer nullBuffer;
    private final AutoExpandingBuffer lengthBuffer;
    private final AutoExpandingBuffer valueBuffer;
    private int rowCount = 0;
    private int nullCount = 0;
    private int tableCount = 0;
    private String lastTableName;

    public Stmt2ColumnFieldBuffer(Stmt2FieldMeta meta) {
        this(meta, null);
    }

    public Stmt2ColumnFieldBuffer(Stmt2FieldMeta meta, BufferSizeHints hints) {
        this.meta = meta;
        if (meta.isVariableWidth()) {
            this.fixedWidthSlab = null;
            this.nullBuffer = new AutoExpandingBuffer(resolveNullBufferInitSize(hints), MAX_COMPONENT_COUNT);
            this.lengthBuffer = new AutoExpandingBuffer(resolveLengthBufferInitSize(hints), MAX_COMPONENT_COUNT);
            this.valueBuffer = new AutoExpandingBuffer(resolveValueBufferInitSize(meta, hints), MAX_COMPONENT_COUNT);
        } else {
            this.fixedWidthSlab = new FixedWidthRawSlab(meta.fixedWidth(), resolveFixedWidthInitialRows(meta, hints));
            this.nullBuffer = null;
            this.lengthBuffer = null;
            this.valueBuffer = null;
        }
    }

    // -----------------------------------------------------------------------
    // Append helpers
    // -----------------------------------------------------------------------

    /** Append a null row for this column. */
    public void appendNull() throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendNull();
        } else {
            nullBuffer.writeByte((byte) 1);
            lengthBuffer.writeIntLE(0);
        }
        rowCount++;
        nullCount++;
    }

    public void appendBool(boolean v) throws SQLException {
        writeFixed1Raw((byte) (v ? 1 : 0));
        rowCount++;
    }

    public void appendTinyInt(byte v) throws SQLException {
        writeFixed1Raw(v);
        rowCount++;
    }

    public void appendUTinyInt(byte v) throws SQLException {
        appendTinyInt(v);
    }

    public void appendSmallInt(short v) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed2(v);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeShort(v);
        }
        rowCount++;
    }

    public void appendUSmallInt(short v) throws SQLException {
        appendSmallInt(v);
    }

    public void appendInt(int v) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed4(v);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeIntLE(v);
        }
        rowCount++;
    }

    public void appendUInt(int v) throws SQLException {
        appendInt(v);
    }

    public void appendBigInt(long v) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed8(v);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeLongLE(v);
        }
        rowCount++;
    }

    public void appendUBigInt(long v) throws SQLException {
        appendBigInt(v);
    }

    public void appendFloat(float v) throws SQLException {
        appendFixed4Raw(Float.floatToRawIntBits(v));
    }

    public void appendDouble(double v) throws SQLException {
        appendFixed8Raw(Double.doubleToRawLongBits(v));
    }

    public void appendTimestamp(long epochValue) throws SQLException {
        appendFixed8Raw(epochValue);
    }

    /**
     * Append already-encoded bytes for variable-width types
     * (BINARY/VARCHAR, NCHAR, JSON, VARBINARY, GEOMETRY, BLOB, DECIMAL*).
     * Pass {@code null} to record a null entry.
     */
    public void appendBytes(byte[] data) throws SQLException {
        appendEncodedVar(data, data == null ? 0 : data.length);
    }

    public void appendString(String value) throws SQLException {
        if (isTbNameColumn()) {
            appendTbNameValue(value);
            return;
        }
        appendUtf8Value(value);
    }

    /**
     * Append a table name for a {@code TAOS_FIELD_TBNAME} column.
     * Encodes the name as UTF-8 bytes and tracks table-count runs incrementally.
     *
     * @throws IllegalStateException if this is not a tbname column
     * @throws IllegalArgumentException if name is null or empty
     */
    public void appendTbName(String name) throws SQLException {
        if (!isTbNameColumn()) {
            throw new IllegalStateException("appendTbName called on non-tbname column");
        }
        appendTbNameValue(name);
    }

    public void appendEncodedVar(byte[] data, int len) throws SQLException {
        if (data == null) {
            appendNull();
            return;
        }
        if (isTbNameColumn()) {
            appendTbNameValue(decodeTbNameBytes(data, len));
            return;
        }
        appendNonNullPrefix();
        lengthBuffer.writeIntLE(len);
        valueBuffer.writeBytes(data, 0, len);
        rowCount++;
    }

    public void appendFixed4Raw(int raw) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed4(raw);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeIntLE(raw);
        }
        rowCount++;
    }

    public void appendFixed8Raw(long raw) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed8(raw);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeLongLE(raw);
        }
        rowCount++;
    }

    private void appendUtf8Value(String value) throws SQLException {
        if (value == null) {
            appendNull();
            return;
        }
        appendNonNullPrefix();
        int utf8Length = valueBuffer.writeString(value);
        lengthBuffer.writeIntLE(utf8Length);
        rowCount++;
    }

    private void appendTbNameValue(String name) throws SQLException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Table name must not be null or empty");
        }
        appendNonNullPrefix();
        int utf8Length = valueBuffer.writeString(name);
        lengthBuffer.writeIntLE(utf8Length);
        rowCount++;
        if (!name.equals(lastTableName)) {
            tableCount++;
            lastTableName = name;
        }
    }

    private String decodeTbNameBytes(byte[] data, int len) throws SQLException {
        if (len <= 0) {
            throw new IllegalArgumentException("Table name must not be null or empty");
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(data, 0, len)).toString();
        } catch (CharacterCodingException e) {
            throw new SQLException("tbname bytes must be valid UTF-8", e);
        }
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    /**
     * Builds and returns the serialized column block bytes.
     *
     * <p>Kept for existing byte-level unit tests and fallback helpers. The hot
     * stmt2 bind-exec path should prefer {@link #buildColumnBlockBuffer()}.
     */
    public byte[] buildColumnBlock() throws SQLException {
        ByteBuf block = buildColumnBlockBuffer();
        try {
            return ByteBufUtil.getBytes(block);
        } finally {
            Utils.releaseByteBuf(block);
        }
    }

    /**
     * Builds and returns the serialized column block buffer.
     *
     * <p>Layout (little-endian):
     * <pre>
     *   4  total_length
     *   4  type
     *   4  num
     *   N  is_null[num]   (1 byte per row)
     *   1  have_length    (1 for variable-width, 0 for fixed)
     *   4N [length[num]]  (only if have_length == 1)
     *   4  buffer_length
     *   *  raw values
     * </pre>
     */
    public ByteBuf buildColumnBlockBuffer() {
        CompositeByteBuf block = PooledByteBufAllocator.DEFAULT.compositeBuffer(blockComponentCount());
        boolean success = false;
        try {
            appendColumnBlockTo(block);
            success = true;
            return block;
        } finally {
            if (!success) {
                if (block != null) {
                    Utils.releaseByteBuf(block);
                }
            }
        }
    }

    int blockComponentCount() {
        return effectiveValueLength() > 0 ? 2 : 1;
    }

    void appendColumnBlockTo(CompositeByteBuf target) {
        boolean varWidth = meta.isVariableWidth();
        int bufferLength = effectiveValueLength();
        int totalLength = getSerializedSize();
        int headerLength = totalLength - bufferLength;
        ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(headerLength);
        ByteBuf nullSlice = null;
        ByteBuf lengthSlice = null;
        ByteBuf valueSlice = null;
        boolean success = false;
        try {
            header.writeIntLE(totalLength);
            header.writeIntLE(meta.getFieldType() & 0xFF);
            header.writeIntLE(rowCount);
            if (rowCount > 0) {
                nullSlice = currentNullSlice();
                header.writeBytes(nullSlice, nullSlice.readerIndex(), nullSlice.readableBytes());
            }
            header.writeByte(varWidth ? 1 : 0);
            if (varWidth && rowCount > 0) {
                lengthSlice = lengthBuffer.retainedDuplicate();
                header.writeBytes(lengthSlice, lengthSlice.readerIndex(), lengthSlice.readableBytes());
            }
            header.writeIntLE(bufferLength);
            target.addComponent(true, header);
            header = null;
            if (bufferLength > 0) {
                valueSlice = currentValueSlice();
                target.addComponent(true, valueSlice);
                valueSlice = null;
            }
            success = true;
        } finally {
            if (nullSlice != null) {
                Utils.releaseByteBuf(nullSlice);
            }
            if (lengthSlice != null) {
                Utils.releaseByteBuf(lengthSlice);
            }
            if (!success) {
                if (header != null) {
                    Utils.releaseByteBuf(header);
                }
                if (valueSlice != null) {
                    Utils.releaseByteBuf(valueSlice);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Table count (tbname columns only)
    // -----------------------------------------------------------------------

    public int computeTableCount() {
        if (meta.getBindType() != (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
            throw new IllegalStateException("computeTableCount called on non-tbname column");
        }
        return tableCount;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public int getRowCount() {
        return rowCount;
    }

    public Stmt2FieldMeta getMeta() {
        return meta;
    }

    public int getSerializedSize() {
        return 17 + rowCount + (meta.isVariableWidth() ? rowCount * Integer.BYTES : 0) + effectiveValueLength();
    }

    public BufferSizeHints snapshotUsage() {
        return new BufferSizeHints(
                fixedWidthSlab != null
                        ? Math.max(NULL_BUFFER_INIT_SIZE, fixedWidthSlab.nullCapacityBytes())
                        : Math.max(NULL_BUFFER_INIT_SIZE, nullBuffer.readableBytes()),
                lengthBuffer == null ? 0 : Math.max(LENGTH_BUFFER_INIT_SIZE, lengthBuffer.readableBytes()),
                fixedWidthSlab != null
                        ? Math.max(resolveValueBufferInitSize(meta, null), fixedWidthSlab.valueCapacityBytes())
                        : Math.max(resolveValueBufferInitSize(meta, null), valueBuffer.readableBytes()));
    }

    public void release() {
        if (nullBuffer != null) {
            nullBuffer.release();
        }
        if (lengthBuffer != null) {
            lengthBuffer.release();
        }
        if (valueBuffer != null) {
            valueBuffer.release();
        }
    }

    public void reset() {
        rowCount = 0;
        nullCount = 0;
        tableCount = 0;
        lastTableName = null;
        if (fixedWidthSlab != null) {
            fixedWidthSlab.reset();
            return;
        }
        nullBuffer.reset();
        if (lengthBuffer != null) {
            lengthBuffer.reset();
        }
        valueBuffer.reset();
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private static int resolveNullBufferInitSize(BufferSizeHints hints) {
        return hints == null ? NULL_BUFFER_INIT_SIZE : Math.max(NULL_BUFFER_INIT_SIZE, hints.nullBytes);
    }

    private static int resolveLengthBufferInitSize(BufferSizeHints hints) {
        return hints == null ? LENGTH_BUFFER_INIT_SIZE : Math.max(LENGTH_BUFFER_INIT_SIZE, hints.lengthBytes);
    }

    private static int resolveValueBufferInitSize(Stmt2FieldMeta meta, BufferSizeHints hints) {
        int defaultSize;
        if (meta.isVariableWidth()) {
            defaultSize = VALUE_BUFFER_INIT_SIZE;
        } else {
            defaultSize = Math.max(1024, meta.fixedWidth() * 1024);
        }
        return hints == null ? defaultSize : Math.max(defaultSize, hints.valueBytes);
    }

    private static int resolveFixedWidthInitialRows(Stmt2FieldMeta meta, BufferSizeHints hints) {
        int nullRows = resolveNullBufferInitSize(hints);
        int valueBytes = resolveValueBufferInitSize(meta, hints);
        int valueRows = (valueBytes + meta.fixedWidth() - 1) / meta.fixedWidth();
        return Math.max(1, Math.max(nullRows, valueRows));
    }

    private void appendNonNullPrefix() throws SQLException {
        nullBuffer.writeByte((byte) 0);
    }

    private void writeFixed1Raw(byte raw) throws SQLException {
        if (fixedWidthSlab != null) {
            fixedWidthSlab.appendFixed1(raw);
        } else {
            appendNonNullPrefix();
            valueBuffer.writeByte(raw);
        }
    }

    private boolean isTbNameColumn() {
        return meta.getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue();
    }

    private ByteBuf currentNullSlice() {
        return fixedWidthSlab != null ? fixedWidthSlab.nullSlice() : nullBuffer.retainedDuplicate();
    }

    private ByteBuf currentValueSlice() {
        return fixedWidthSlab != null ? fixedWidthSlab.valueSlice() : valueBuffer.retainedDuplicate();
    }

    private int effectiveValueLength() {
        if (nullCount == rowCount) {
            return 0;
        }
        return fixedWidthSlab != null ? rowCount * meta.fixedWidth() : valueBuffer.readableBytes();
    }
}
