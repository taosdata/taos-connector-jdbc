package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.sql.SQLException;

/**
 * Builds the binary payload for the {@code stmt2_bind_exec} WebSocket action.
 *
 * <h3>Payload layout (all fields little-endian)</h3>
 * <pre>
 * Header (20 bytes):
 *   4  total_length
 *   4  row_count
 *   4  table_count
 *   4  field_count
 *   4  field_offset   (always 20 – points to start of column blocks)
 *
 * Per-column block (see Stmt2ColumnFieldBuffer.buildColumnBlock()):
 *   4  total_length
 *   4  type
 *   4  num
 *   N  is_null[num]   (1 byte per row)
 *   1  have_length
 *  4N  [length[num]]  (only when have_length == 1)
 *   4  buffer_length
 *   *  raw values
 * </pre>
 *
 * This class is pure format logic; it does not activate any new statement path.
 */
public final class Stmt2ColumnBindSerializer {

    /** Offset of total_length in the payload header. */
    public static final int HEADER_TOTAL_LENGTH_OFFSET = 0;
    /** Offset of row_count in the payload header. */
    public static final int HEADER_ROW_COUNT_OFFSET = 4;
    /** Offset of table_count in the payload header. */
    public static final int HEADER_TABLE_COUNT_OFFSET = 8;
    /** Offset of field_count in the payload header. */
    public static final int HEADER_FIELD_COUNT_OFFSET = 12;
    /** Offset of field_offset in the payload header. */
    public static final int HEADER_FIELD_OFFSET_OFFSET = 16;
    /** Byte length of the payload header; column blocks start immediately after. */
    public static final int HEADER_SIZE = 20;

    private Stmt2ColumnBindSerializer() {
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Serialize a set of column field buffers into the binary payload.
     *
     * <p>The buffers must all contain the same number of rows and must be in
     * field prepare-order.  When a {@code TAOS_FIELD_TBNAME} buffer is present,
     * {@code table_count} is derived from it; otherwise it defaults to 1.
     *
     * <p>If any field has {@code bindType == TAOS_FIELD_QUERY} the row count
     * must be exactly 1.
     *
     * @param columns one buffer per stmt field in prepare order
     * @return the complete payload bytes ready to send to taosadapter
     * @throws SQLException if row counts are inconsistent or query constraint is violated
     */
    public static byte[] serialize(Stmt2ColumnFieldBuffer[] columns) throws SQLException {
        ByteBuf payload = serializeBuffer(columns);
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            Utils.releaseByteBuf(payload);
        }
    }

    public static ByteBuf serializeBuffer(Stmt2ColumnFieldBuffer[] columns) throws SQLException {
        if (columns == null || columns.length == 0) {
            throw new SQLException("columns must not be null or empty");
        }

        int rowCount = columns[0].getRowCount();
        int tbNameIndex = -1;
        boolean hasQueryField = false;

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getRowCount() != rowCount) {
                throw new SQLException(
                        "row count mismatch at column " + i
                                + ": expected " + rowCount
                                + ", got " + columns[i].getRowCount());
            }
            byte bindType = columns[i].getMeta().getBindType();
            if (bindType == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                if (tbNameIndex == -1) {
                    tbNameIndex = i;
                }
            } else if (bindType == (byte) FieldBindType.TAOS_FIELD_QUERY.getValue()) {
                hasQueryField = true;
            }
        }

        if (hasQueryField && rowCount != 1) {
            throw new SQLException("query bind only supports one row, got " + rowCount);
        }

        int tableCount = 1;
        if (tbNameIndex >= 0) {
            tableCount = columns[tbNameIndex].computeTableCount();
        }

        return buildPayloadBuffer(columns, rowCount, tableCount);
    }

    /**
     * Serialize columns for a query bind (no field-type metadata available).
     * Row count must be exactly 1.
     *
     * @param columns one buffer per query parameter in prepare order
     * @return the complete payload bytes
     * @throws SQLException if row count is not 1
     */
    public static byte[] serializeQuery(Stmt2ColumnFieldBuffer[] columns) throws SQLException {
        ByteBuf payload = serializeQueryBuffer(columns);
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            Utils.releaseByteBuf(payload);
        }
    }

    public static ByteBuf serializeQueryBuffer(Stmt2ColumnFieldBuffer[] columns) throws SQLException {
        if (columns == null || columns.length == 0) {
            throw new SQLException("columns must not be null or empty");
        }
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getRowCount() != 1) {
                throw new SQLException(
                        "query bind only supports one row; column " + i
                                + " has " + columns[i].getRowCount() + " rows");
            }
        }
        return buildPayloadBuffer(columns, 1, 1);
    }

    public static ByteBuf serializeBuffer(
            Stmt2ColumnFieldBuffer[] columns,
            int tableCount) throws SQLException {
        if (columns == null || columns.length == 0) {
            throw new SQLException("columns must not be null or empty");
        }
        int rowCount = columns[0].getRowCount();
        boolean hasQueryField = false;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getRowCount() != rowCount) {
                throw new SQLException(
                        "row count mismatch at column " + i
                                + ": expected " + rowCount
                                + ", got " + columns[i].getRowCount());
            }
            if (columns[i].getMeta().getBindType() == (byte) FieldBindType.TAOS_FIELD_QUERY.getValue()) {
                hasQueryField = true;
            }
        }
        if (hasQueryField && rowCount != 1) {
            throw new SQLException("query bind only supports one row, got " + rowCount);
        }
        return buildPayloadBuffer(columns, rowCount, tableCount);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private static ByteBuf buildPayloadBuffer(
            Stmt2ColumnFieldBuffer[] columns,
            int rowCount,
            int tableCount) {
        int fieldCount = columns.length;
        int componentCount = 1;
        for (Stmt2ColumnFieldBuffer column : columns) {
            componentCount += column.blockComponentCount();
        }
        CompositeByteBuf payload = PooledByteBufAllocator.DEFAULT.compositeBuffer(componentCount);
        boolean success = false;
        try {
            ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(HEADER_SIZE);
            int totalLength = HEADER_SIZE;
            for (int i = 0; i < fieldCount; i++) {
                totalLength += columns[i].getSerializedSize();
            }

            header.writeIntLE(totalLength);
            header.writeIntLE(rowCount);
            header.writeIntLE(tableCount);
            header.writeIntLE(fieldCount);
            header.writeIntLE(HEADER_SIZE);
            payload.addComponent(true, header);
            for (Stmt2ColumnFieldBuffer column : columns) {
                column.appendColumnBlockTo(payload);
            }
            success = true;
            return payload;
        } finally {
            if (!success) {
                Utils.releaseByteBuf(payload);
            }
        }
    }
}
