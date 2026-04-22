package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only accumulator for a single stmt2 column field.
 *
 * One buffer is kept per prepared-statement field.  Callers append row values
 * one at a time; the buffer then serialises the complete per-column block
 * (totalLength / type / num / isNull[] / haveLength / [length[]] / bufferLength / raw values)
 * on demand via {@link #buildColumnBlock()}.
 *
 * Phase 1 uses one byte-per-row null markers (not a bitmap).
 */
public final class Stmt2ColumnFieldBuffer {

    private final Stmt2FieldMeta meta;
    private int rowCount = 0;

    // null markers – 1 byte per row (1 = null, 0 = not null)
    private byte[] nullBytes = new byte[16];

    // per-row byte lengths for variable-width columns; null for fixed-width
    private int[] rowLengths;

    // raw value bytes
    private final ByteArrayOutputStream values = new ByteArrayOutputStream(64);

    // kept only for TAOS_FIELD_TBNAME columns; used to derive table count
    private final List<String> tableNames;

    public Stmt2ColumnFieldBuffer(Stmt2FieldMeta meta) {
        this.meta = meta;
        if (meta.isVariableWidth()) {
            this.rowLengths = new int[16];
        }
        this.tableNames =
                meta.getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()
                        ? new ArrayList<>()
                        : null;
    }

    // -----------------------------------------------------------------------
    // Append helpers
    // -----------------------------------------------------------------------

    /** Append a null row for this column. */
    public void appendNull() {
        ensureCapacity();
        nullBytes[rowCount] = 1;
        if (rowLengths != null) {
            rowLengths[rowCount] = 0;
        }
        // Fixed-width: placeholder zero bytes so the value slot is reserved.
        // (All-null case discards them in buildColumnBlock.)
        if (!meta.isVariableWidth()) {
            int w = meta.fixedWidth();
            for (int i = 0; i < w; i++) {
                values.write(0);
            }
        }
        rowCount++;
    }

    public void appendBool(boolean v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        values.write(v ? 1 : 0);
    }

    public void appendTinyInt(byte v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        values.write(v & 0xFF);
    }

    public void appendUTinyInt(byte v) {
        appendTinyInt(v);
    }

    public void appendSmallInt(short v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE16(values, v);
    }

    public void appendUSmallInt(short v) {
        appendSmallInt(v);
    }

    public void appendInt(int v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE32(values, v);
    }

    public void appendUInt(int v) {
        appendInt(v);
    }

    public void appendBigInt(long v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE64(values, v);
    }

    public void appendUBigInt(long v) {
        appendBigInt(v);
    }

    public void appendFloat(float v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE32(values, Float.floatToRawIntBits(v));
    }

    public void appendDouble(double v) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE64(values, Double.doubleToRawLongBits(v));
    }

    public void appendTimestamp(long epochValue) {
        ensureCapacity();
        nullBytes[rowCount++] = 0;
        writeLE64(values, epochValue);
    }

    /**
     * Append already-encoded bytes for variable-width types
     * (BINARY/VARCHAR, NCHAR, JSON, VARBINARY, GEOMETRY, BLOB, DECIMAL*).
     * Pass {@code null} to record a null entry.
     */
    public void appendBytes(byte[] data) {
        if (data == null) {
            appendNull();
            return;
        }
        ensureCapacity();
        nullBytes[rowCount] = 0;
        rowLengths[rowCount] = data.length;
        values.write(data, 0, data.length);
        rowCount++;
    }

    /**
     * Append a table name for a {@code TAOS_FIELD_TBNAME} column.
     * Encodes the name as UTF-8 bytes and tracks it for table-count derivation.
     *
     * @throws IllegalStateException if this is not a tbname column
     * @throws IllegalArgumentException if name is null or empty
     */
    public void appendTbName(String name) {
        if (tableNames == null) {
            throw new IllegalStateException("appendTbName called on non-tbname column");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Table name must not be null or empty");
        }
        byte[] data = name.getBytes(StandardCharsets.UTF_8);
        ensureCapacity();
        nullBytes[rowCount] = 0;
        rowLengths[rowCount] = data.length;
        values.write(data, 0, data.length);
        tableNames.add(name);
        rowCount++;
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    /**
     * Builds and returns the serialised column block bytes.
     *
     * Layout (little-endian):
     * <pre>
     *   4  total_length
     *   4  type
     *   4  num
     *   N  is_null[num]   (1 byte per row)
     *   1  have_length    (1 for variable-width, 0 for fixed)
     *   4N [length[num]]  (only if have_length == 1, int32 LE per row)
     *   4  buffer_length
     *   *  raw values
     * </pre>
     */
    public byte[] buildColumnBlock() throws SQLException {
        int num = rowCount;
        boolean varWidth = meta.isVariableWidth();
        boolean allNull = isAllNull(num);

        // header = 4+4+4 + num + 1 + (varWidth ? num*4 : 0) + 4
        int headerSize = 17 + num + (varWidth ? num * 4 : 0);

        byte[] valueData;
        if (allNull) {
            // No value bytes when all rows are null (matches adapter behaviour)
            valueData = new byte[0];
        } else {
            valueData = values.toByteArray();
        }
        int bufferLength = valueData.length;
        int totalLength = headerSize + bufferLength;

        ByteArrayOutputStream out = new ByteArrayOutputStream(totalLength);

        writeLE32(out, totalLength);                    // total_length
        writeLE32(out, meta.getFieldType() & 0xFF);     // type
        writeLE32(out, num);                            // num
        out.write(nullBytes, 0, num);                   // is_null[num]
        out.write(varWidth ? 1 : 0);                    // have_length
        if (varWidth) {
            for (int i = 0; i < num; i++) {
                writeLE32(out, rowLengths[i]);           // length[i]
            }
        }
        writeLE32(out, bufferLength);                   // buffer_length
        if (bufferLength > 0) {
            out.write(valueData, 0, bufferLength);       // raw values
        }

        return out.toByteArray();
    }

    // -----------------------------------------------------------------------
    // Table count (tbname columns only)
    // -----------------------------------------------------------------------

    /**
     * Counts sequential runs of distinct table names, matching the adapter's
     * stmt2ColumnTableCount logic.
     *
     * @throws IllegalStateException if this is not a TAOS_FIELD_TBNAME column
     */
    public int computeTableCount() {
        if (tableNames == null) {
            throw new IllegalStateException("computeTableCount called on non-tbname column");
        }
        if (tableNames.isEmpty()) {
            return 0;
        }
        int count = 1;
        String last = tableNames.get(0);
        for (int i = 1; i < tableNames.size(); i++) {
            String name = tableNames.get(i);
            if (!name.equals(last)) {
                count++;
                last = name;
            }
        }
        return count;
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

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private void ensureCapacity() {
        if (rowCount < nullBytes.length) {
            return;
        }
        int newLen = nullBytes.length * 2;
        byte[] newNulls = new byte[newLen];
        System.arraycopy(nullBytes, 0, newNulls, 0, rowCount);
        nullBytes = newNulls;
        if (rowLengths != null) {
            int[] newLengths = new int[newLen];
            System.arraycopy(rowLengths, 0, newLengths, 0, rowCount);
            rowLengths = newLengths;
        }
    }

    private boolean isAllNull(int num) {
        for (int i = 0; i < num; i++) {
            if (nullBytes[i] == 0) {
                return false;
            }
        }
        return true;
    }

    private static void writeLE16(ByteArrayOutputStream out, short v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void writeLE32(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }

    private static void writeLE64(ByteArrayOutputStream out, long v) {
        out.write((int)(v & 0xFF));
        out.write((int)((v >>> 8) & 0xFF));
        out.write((int)((v >>> 16) & 0xFF));
        out.write((int)((v >>> 24) & 0xFF));
        out.write((int)((v >>> 32) & 0xFF));
        out.write((int)((v >>> 40) & 0xFF));
        out.write((int)((v >>> 48) & 0xFF));
        out.write((int)((v >>> 56) & 0xFF));
    }
}
