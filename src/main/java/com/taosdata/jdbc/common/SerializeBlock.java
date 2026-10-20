/*
 * Copyright (c) 2023 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.toUnsignedLongBits;

public class SerializeBlock {
    private SerializeBlock() {
    }
    private static void serializeShort(ByteBuf buf, short v){
        buf.writeShortLE(v);
    }

    /**
     * Returns the numeric value of a bound object for serialization into a numeric
     * column. The bound runtime type may differ from the column type (e.g. Apache
     * Spark binds ByteType/ShortType via setInt). A Boolean binds as 1/0, matching
     * the stmt2 column-bind path (WSEWColumnBufferWriter); any other non-Number
     * type is rejected with SQLException instead of a bare ClassCastException.
     */
    private static Number toNumber(Object o) throws SQLException {
        if (o instanceof Number) {
            return (Number) o;
        }
        if (o instanceof Boolean) {
            return (Boolean) o ? 1 : 0;
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                "unsupported data type : " + (o == null ? "null" : o.getClass().getName()));
    }

    /**
     * Returns the boolean value of a bound object for serialization into a BOOL
     * column. A Number binds as intValue() != 0, matching the stmt2 column-bind
     * path (WSEWColumnBufferWriter).
     */
    private static boolean toBoolean(Object o) throws SQLException {
        if (o instanceof Boolean) {
            return (Boolean) o;
        }
        if (o instanceof Number) {
            return ((Number) o).intValue() != 0;
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                "unsupported data type : " + (o == null ? "null" : o.getClass().getName()));
    }

    private static SQLException outOfRange(String typeName) {
        return TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                typeName + " value is out of range");
    }

    /**
     * Narrows a bound object to a long within [min, max], raising SQLException on
     * overflow. This is the SQL-standard SQLSTATE 22003 (numeric_value_out_of_range)
     * behavior: PostgreSQL, Derby and H2 all reject out-of-range narrowing instead
     * of wrapping. BigInteger/BigDecimal are compared exactly (their longValue()
     * would wrap); Float/Double saturate on longValue(), so the range check still
     * triggers, and non-finite inputs are rejected outright since no integer column
     * can hold them.
     */
    private static long toExactLong(Object o, long min, long max, String typeName) throws SQLException {
        Number n = toNumber(o);
        long v;
        if (n instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) n;
            if (bd.compareTo(BigDecimal.valueOf(min)) < 0 || bd.compareTo(BigDecimal.valueOf(max)) > 0) {
                throw outOfRange(typeName);
            }
            v = bd.longValue();
        } else if (n instanceof BigInteger) {
            BigInteger bi = (BigInteger) n;
            if (bi.compareTo(BigInteger.valueOf(min)) < 0 || bi.compareTo(BigInteger.valueOf(max)) > 0) {
                throw outOfRange(typeName);
            }
            v = bi.longValue();
        } else {
            if (n instanceof Double || n instanceof Float) {
                double d = n.doubleValue();
                // reject NaN/Infinity and values beyond the long range; longValue()
                // would silently map them to 0 or saturate to Long.MIN/MAX_VALUE
                if (!Double.isFinite(d) || d < -0x1p63 || d >= 0x1p63) {
                    throw outOfRange(typeName);
                }
            }
            v = n.longValue();
            if (v < min || v > max) {
                throw outOfRange(typeName);
            }
        }
        return v;
    }

    private static float toExactFloat(Object o) throws SQLException {
        Number n = toNumber(o);
        float v = n.floatValue();
        // narrowing a finite value must not silently become Infinity; an input that
        // is already NaN/Infinity is preserved, matching the stmt2 column-bind path.
        // BigInteger/BigDecimal are always finite, though their doubleValue() may
        // saturate to Infinity.
        if (Float.isInfinite(v) && (n instanceof BigInteger || n instanceof BigDecimal
                || Double.isFinite(n.doubleValue()))) {
            throw outOfRange("float");
        }
        return v;
    }

    /**
     * Converts a bound object to BigInteger for the UBIGINT branch, where the range
     * check happens in toUnsignedLongBits. Float/Double may exceed the long range,
     * so they go through BigDecimal instead of a saturating longValue().
     */
    private static BigInteger toBigInteger(Object o) throws SQLException {
        Number n = toNumber(o);
        if (n instanceof BigInteger) {
            return (BigInteger) n;
        }
        if (n instanceof BigDecimal) {
            return ((BigDecimal) n).toBigInteger();
        }
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            if (!Double.isFinite(d)) {
                throw outOfRange("ubigint");
            }
            return BigDecimal.valueOf(d).toBigInteger();
        }
        return BigInteger.valueOf(n.longValue());
    }

    public static void serializeByteArray(ByteBuf buf, byte[] data) {
        buf.writeBytes(data);
    }

    private static void serializeColumn(ColumnInfo columnInfo, ByteBuf buf, int precision) throws SQLException {
        Integer dataLen = DataLengthCfg.getDataLength(columnInfo.getType());

        // TotalLength
        buf.writeIntLE(columnInfo.getSerializeSize());

        // Type
        buf.writeIntLE(columnInfo.getType());
        // Num
        buf.writeIntLE(columnInfo.getDataList().size());
        // IsNull
        for (Object o: columnInfo.getDataList()) {
            if (o == null) {
                buf.writeByte(1);
            } else {
                buf.writeByte(0);
            }
        }

        // haveLength
        if (dataLen != null){
            buf.writeByte(0);
            // buffer
            serializeNormalDataType(columnInfo.getType(), buf, columnInfo.getDataList(), precision);
            return;
        }

        // data is array type
        buf.writeByte(1);
        // length
        int bufferLength = 0;
        for (Object o: columnInfo.getDataList()){
            if (o == null){
                buf.writeIntLE(0);
            } else {
                switch (columnInfo.getType()) {
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_BLOB:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    case TSDB_DATA_TYPE_DECIMAL64:
                    case TSDB_DATA_TYPE_DECIMAL128:
                    case TSDB_DATA_TYPE_NCHAR:{
                        byte[] v = (byte[]) o;
                        buf.writeIntLE(v.length);
                        bufferLength += v.length;
                        break;
                    }
                    default:
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + columnInfo.getType());
                }
            }
        }

        // buffer length
        buf.writeIntLE(bufferLength);

        // buffer
        serializeArrayDataType(columnInfo.getType(), buf, columnInfo.getDataList());
        return;
    }

    private static void serializeNormalDataType(int dataType , ByteBuf buf, List<Object> objectList, int precision) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BOOL: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        buf.writeByte(toBoolean(o) ? 1 : 0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_TINYINT: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        buf.writeByte((byte) toExactLong(o, Byte.MIN_VALUE, Byte.MAX_VALUE, "tinyint"));
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        buf.writeByte((byte) toExactLong(o, 0, MAX_UNSIGNED_BYTE, "utinyint"));
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
                buf.writeIntLE(objectList.size() * Short.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        serializeShort(buf, (short) toExactLong(o, Short.MIN_VALUE, Short.MAX_VALUE, "smallint"));
                    } else {
                        serializeShort(buf, (short)0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                buf.writeIntLE(objectList.size() * Short.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeShortLE((short) toExactLong(o, 0, MAX_UNSIGNED_SHORT, "usmallint"));
                    } else {
                        buf.writeShortLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_INT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeIntLE((int) toExactLong(o, Integer.MIN_VALUE, Integer.MAX_VALUE, "int"));
                    } else {
                        buf.writeIntLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_UINT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeIntLE((int) toExactLong(o, 0, MAX_UNSIGNED_INT, "uint"));
                    } else {
                        buf.writeIntLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeLongLE(toExactLong(o, Long.MIN_VALUE, Long.MAX_VALUE, "bigint"));
                    } else {
                        buf.writeLongLE(0L);
                    }
                }
               break;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeLongLE(toUnsignedLongBits(toBigInteger(o)));
                    } else {
                        buf.writeLongLE(0L);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    float v = 0;
                    if (o != null) {
                        v = toExactFloat(o);
                    }
                    buf.writeFloatLE(v);
                }
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    double v = 0;
                    if (o != null) {
                        v = toNumber(o).doubleValue();
                    }
                    buf.writeDoubleLE(v);
                }
                break;
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        if (o instanceof Instant){
                            Instant instant = (Instant) o;
                            long v = DateTimeUtils.toLong(instant, precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof OffsetDateTime){
                            OffsetDateTime offsetDateTime = (OffsetDateTime) o;
                            long v = DateTimeUtils.toLong(offsetDateTime.toInstant(), precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof ZonedDateTime){
                            ZonedDateTime zonedDateTime = (ZonedDateTime) o;
                            long v = DateTimeUtils.toLong(zonedDateTime.toInstant(), precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof Long){
                            buf.writeLongLE((Long) o);
                        } else {
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported timestamp data type : " + o.getClass().getName());
                        }

                    } else {
                        buf.writeLongLE(0L);
                    }
                }
                break;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + dataType);
        }
    }

    private static void serializeArrayDataType(int dataType, ByteBuf buf, List<Object> objectList) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR: {
                for (Object o: objectList){
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        serializeByteArray(buf, v);
                    }
                }
                break;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + dataType);
        }
    }
    public static int getTagTotalLength(TableInfo tableInfo, int toBebindTagCount) throws SQLException{
        int totalLength = 0;
        if (toBebindTagCount > 0){
            if (tableInfo.getTagInfo().size() != toBebindTagCount){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table tag size is not match");
            }

            for (ColumnInfo tag : tableInfo.getTagInfo()){
                if (tag.getDataList().isEmpty()){
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "tag value is null, tbname: " + tableInfo.getTableName().toString());
                }
                int columnSize = getColumnSize(tag);
                tag.setSerializeSize(columnSize);
                totalLength += columnSize;
            }
        }
        return totalLength;
    }

    public static int getColTotalLength(TableInfo tableInfo, int toBebindColCount) throws SQLException{
        int totalLength = 0;
        if (toBebindColCount > 0){
            if (tableInfo.getDataList().size() != toBebindColCount){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table column size is not match");
            }

            for (ColumnInfo columnInfo : tableInfo.getDataList()){
                int columnSize = getColumnSize(columnInfo);
                columnInfo.setSerializeSize(columnSize);
                totalLength += columnSize;
            }
        }
        return totalLength;
    }

    public static int getColumnSize(ColumnInfo column) throws SQLException {
        Integer dataLen = DataLengthCfg.getDataLength(column.getType());

        if (dataLen != null) {
            // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + size * dataLen
            return 17 + (dataLen + 1) * column.getDataList().size();
        }

        switch (column.getType()) {
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR:{
                int totalLength = 0;
                for (Object o : column.getDataList()) {
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        totalLength += v.length;
                    }
                }
                // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 * v.length + totalLength
                return 17 + (5 * column.getDataList().size()) + totalLength;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + column.getType());
        }
    }

    public static ByteBuf getStmt2BindBlock(HashMap<ByteBuffer, TableInfo> tableInfoMap,
                                            StmtInfo stmtInfo,
                                            long reqId) throws SQLException {
        return getStmt2BindBlock(reqId,
                stmtInfo.getStmtId(),
                tableInfoMap,
                stmtInfo.getToBeBindTableNameIndex(),
                stmtInfo.getToBeBindTagCount(),
                stmtInfo.getToBeBindColCount(),
                stmtInfo.getPrecision());
    }
    public static ByteBuf getStmt2BindBlock(long reqId,
                                           long stmtId,
                                           HashMap<ByteBuffer, TableInfo> tableInfoMap,
                                           int toBeBindTableNameIndex,
                                           int toBebindTagCount,
                                           int toBebindColCount,
                                           int precision) throws SQLException {

        // cloc totol size
        int totalTableNameSize  = 0;
        List<Short> tableNameSizeList = new ArrayList<>();
        if (toBeBindTableNameIndex >= 0) {
            for (TableInfo tableInfo: tableInfoMap.values()) {
                if (tableInfo.getTableName().capacity() == 0){
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                }
                int tableNameSize = tableInfo.getTableName().capacity() + 1;
                totalTableNameSize += tableNameSize;
                tableNameSizeList.add((short) tableNameSize);
            }
        }

        int totalTagSize = 0;
        List<Integer> tagSizeList = new ArrayList<>();
        if (toBebindTagCount > 0){
            for (TableInfo tableInfo : tableInfoMap.values()) {
                int tagSize = getTagTotalLength(tableInfo, toBebindTagCount);
                totalTagSize += tagSize;
                tagSizeList.add(tagSize);
            }
        }


        int totalColSize = 0;
        List<Integer> colSizeList = new ArrayList<>();
        if (toBebindColCount > 0) {
            for (TableInfo tableInfo : tableInfoMap.values()) {
                int colSize = getColTotalLength(tableInfo, toBebindColCount);
                totalColSize += colSize;
                colSizeList.add(colSize);
            }
        }

        int totalSize = totalTableNameSize + totalTagSize + totalColSize;
        int toBebindTableNameCount = toBeBindTableNameIndex >= 0 ? 1 : 0;

        totalSize += tableInfoMap.size() * (
                toBebindTableNameCount * Short.BYTES
                + (toBebindTagCount > 0 ? 1 : 0) * Integer.BYTES
                + (toBebindColCount > 0 ? 1 : 0) * Integer.BYTES);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(58 + totalSize);

        try {
            //************ header *****************
            // ReqId
            buf.writeLongLE(reqId);
            // stmtId
            buf.writeLongLE(stmtId);
            // actionId
            buf.writeLongLE(9L);
            // version
            buf.writeShortLE(1);
            // col_idx
            buf.writeIntLE(-1);

            //************ data *****************
            // TotalLength
            buf.writeIntLE(totalSize + 28);
            // tableCount
            buf.writeIntLE(tableInfoMap.size());
            // TagCount
            buf.writeIntLE(toBebindTagCount);
            // ColCount
            buf.writeIntLE(toBebindColCount);
            // tableNameOffset
            if (toBebindTableNameCount > 0) {
                buf.writeIntLE(0x1C);
            } else {
                buf.writeIntLE(0);
            }

            // tagOffset
            if (toBebindTagCount > 0) {
                if (toBebindTableNameCount > 0) {
                    buf.writeIntLE(28 + totalTableNameSize + Short.BYTES * tableInfoMap.size());
                } else {
                    buf.writeIntLE(28);
                }
            } else {
                buf.writeIntLE(0);
            }

            // colOffset
            if (toBebindColCount > 0) {
                int skipSize = 0;
                if (toBebindTableNameCount > 0) {
                    skipSize += totalTableNameSize + Short.BYTES * tableInfoMap.size();
                }

                if (toBebindTagCount > 0) {
                    skipSize += totalTagSize + Integer.BYTES * tableInfoMap.size();
                }
                buf.writeIntLE(28 + skipSize);
            } else {
                buf.writeIntLE(0);
            }

            // TableNameLength
            if (toBebindTableNameCount > 0) {
                for (Short tableNameLen : tableNameSizeList) {
                    if (tableNameLen == 0) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                    }

                    buf.writeShortLE(tableNameLen);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    if (tableInfo.getTableName().capacity() == 0) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                    }

                    buf.writeBytes(tableInfo.getTableName().array());
                    buf.writeByte(0);
                }
            }

            // TagsDataLength
            if (toBebindTagCount > 0) {
                for (Integer tagsize : tagSizeList) {
                    buf.writeIntLE(tagsize);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    for (ColumnInfo tag : tableInfo.getTagInfo()) {
                        if (tag.getDataList().isEmpty()) {
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "tag value is null, tbname: " + tableInfo.getTableName().toString());
                        }
                        serializeColumn(tag, buf, precision);
                    }
                }
            }

            // ColsDataLength
            if (toBebindColCount > 0) {
                for (Integer colSize : colSizeList) {
                    buf.writeIntLE(colSize);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    for (ColumnInfo col : tableInfo.getDataList()) {
                        serializeColumn(col, buf, precision);
                    }
                }
            }


//        for (int i = 30; i < buf.capacity(); i++) { // NOSONAR
//            int bb = buf.getByte(i) & 0xff;
//            System.out.print(bb);
//            System.out.print(",");
//        }
//        System.out.println();
            return buf;
        } catch (Exception e){
            Utils.releaseByteBuf(buf);
            throw e;
        }
    }
}
