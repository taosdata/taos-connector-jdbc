package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.DateTimeUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

public class SerializeBlock {
    private SerializeBlock() {
    }

    private static int bitMapLen(int n) {
        return ((n) + ((1 << 3) - 1)) >> 3;
    }

    private static int bitPos(int n) {
        return n & ((1 << 3) - 1);
    }

    private static int charOffset(int n) {
        return n >> 3;
    }

    private static byte bmSetNull(byte c, int n) {
        return (byte) (c + (1 << (7 - bitPos(n))));
    }

    private static void handleBoolean(ByteArrayOutputStream buf, Object o){
        boolean v = (Boolean) o;
        if (v) {
            buf.write(1);
        }else {
            buf.write(0);
        }
    }
    private static void SerializeInt(byte[] buf, int offset, int v){
        buf[offset] = (byte) (v & 0xFF);
        buf[offset + 1] = (byte) ((v >> 8) & 0xFF);
        buf[offset + 2] = (byte) ((v >> 16) & 0xFF);
        buf[offset + 3] = (byte) ((v >> 24) & 0xFF);
    }

    private static void SerializeLong(byte[] buf, int offset, long v){
        buf[offset] = (byte) (v & 0xFF);
        buf[offset + 1] = (byte) ((v >> 8) & 0xFF);
        buf[offset + 2] = (byte) ((v >> 16) & 0xFF);
        buf[offset + 3] = (byte) ((v >> 24) & 0xFF);
        buf[offset + 4] = (byte) ((v >> 32) & 0xFF);
        buf[offset + 5] = (byte) ((v >> 40) & 0xFF);
        buf[offset + 6] = (byte) ((v >> 48) & 0xFF);
        buf[offset + 7] = (byte) ((v >> 56) & 0xFF);
    }
    private static void SerializeShort(byte[] buf, int offset, short v){
        buf[offset] = (byte) (v & 0xFF);
        buf[offset + 1] = (byte) ((v >> 8) & 0xFF);
    }

    public static void serializeByteArray(byte[] buf, int offset, byte[] data) {
        System.arraycopy(data, 0, buf, offset, data.length);
    }

    private static int serializeColumn(ColumnInfo columnInfo, byte[] buf, int offset, int precision) throws IOException, SQLException {
        Integer dataLen = DataLengthCfg.getDataLength(columnInfo.getType());

        // TotalLength
        SerializeInt(buf, offset, columnInfo.getSerializeSize());
        offset += Integer.BYTES;

        // Type
        SerializeInt(buf, offset, columnInfo.getType());
        offset += Integer.BYTES;

        // Num
        SerializeInt(buf, offset, columnInfo.getDataList().size());
        offset += Integer.BYTES;

        // IsNull
        for (Object o: columnInfo.getDataList()) {
            if (o == null) {
                buf[offset++] = 1;
            } else {
                buf[offset++] = 0;
            }
        }

        // haveLength
        if (dataLen != null){
            buf[offset++] = 0;
            // buffer
            SerializeNormalDataType(columnInfo.getType(), buf, offset, columnInfo.getDataList(), precision);
            return offset;
        }

        // data is array type
        buf[offset++] = 1;
        // length
        int bufferLength = 0;
        for (Object o: columnInfo.getDataList()){
            if (o == null){
                SerializeInt(buf, offset, 0);
                offset += Integer.BYTES;
            } else {
                switch (columnInfo.getType()) {
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:{
                        byte[] v = (byte[]) o;
                        SerializeInt(buf, offset, v.length);
                        offset += Integer.BYTES;
                        bufferLength += v.length;
                        break;
                    }
                    case TSDB_DATA_TYPE_NCHAR: {
                        String v = (String) o;
                        int len = v.getBytes().length;
                        SerializeInt(buf, offset, len);
                        offset += Integer.BYTES;
                        bufferLength += len;
                        break;
                    }
                    default:
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + columnInfo.getType());
                }
            }
        }

        // buffer length
        SerializeInt(buf, offset, bufferLength);
        offset += Integer.BYTES;

        // buffer
        SerializeArrayDataType(columnInfo.getType(), buf, offset, columnInfo.getDataList());
        return offset;
    }

    private static void SerializeNormalDataType(int dataType , byte[] buf, int offset, List<Object> objectList, int precision) throws IOException, SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BOOL: {
                SerializeInt(buf, offset, objectList.size());
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o == null) {
                        buf[offset++] = 0;
                    } else {
                        boolean v = (Boolean) o;
                        buf[offset++] = v ? (byte) 1 : (byte) 0;
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_TINYINT: {
                SerializeInt(buf, offset, objectList.size());
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o == null) {
                        buf[offset++] = 0;
                    } else {
                        byte v = (Byte) o;
                        buf[offset++] = v;
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                SerializeInt(buf, offset, objectList.size());
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o == null) {
                        buf[offset++] = 0;
                    } else {
                        short v = (Short) o;
                        if (v < 0 || v > MAX_UNSIGNED_BYTE){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "utinyint value is out of range");
                        }
                        buf[offset++] = (byte) (v & 0xFF);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
                SerializeInt(buf, offset, objectList.size() * Short.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        SerializeShort(buf, offset, (Short) o);
                    } else {
                        SerializeShort(buf, offset, (short)0);
                    }

                    offset += Short.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                SerializeInt(buf, offset, objectList.size() * Short.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        int v = (Integer) o;
                        if (v < 0 || v > MAX_UNSIGNED_SHORT){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "usmallint value is out of range");
                        }
                        SerializeShort(buf, offset, (short) (v & 0xFFFF));
                    } else {
                        SerializeShort(buf, offset, (short)0);
                    }

                    offset += Short.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_INT: {
                SerializeInt(buf, offset, objectList.size() * Integer.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        SerializeInt(buf, offset, (Integer) o);
                    } else {
                        SerializeInt(buf, offset, 0);
                    }
                    offset += Integer.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_UINT: {
                SerializeInt(buf, offset, objectList.size() * Integer.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        long v = (Long) o;
                        if (v < 0 || v > MAX_UNSIGNED_INT){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "uint value is out of range");
                        }
                        SerializeInt(buf, offset, (int) (v & 0xFFFFFFFFL));
                    } else {
                        SerializeInt(buf, offset, 0);
                    }
                    offset += Integer.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                SerializeInt(buf, offset, objectList.size() * Long.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        SerializeLong(buf, offset, (Long)o);
                    } else {
                        SerializeLong(buf, offset, 0L);
                    }
                    offset += Long.BYTES;
                }
               break;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                SerializeInt(buf, offset, objectList.size() * Long.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        BigInteger v = (BigInteger) o;
                        if (v.compareTo(BigInteger.ZERO) < 0 || v.compareTo(new BigInteger(MAX_UNSIGNED_LONG)) > 0){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
                        }
                        SerializeLong(buf, offset, v.longValue());
                    } else {
                        SerializeLong(buf, offset, 0L);
                    }
                    offset += Long.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                SerializeInt(buf, offset, objectList.size() * Integer.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    float v = 0;
                    if (o != null) {
                        v = (Float) o;
                    }
                    int f = Float.floatToIntBits(v);
                    SerializeInt(buf, offset, f);
                    offset += Integer.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                SerializeInt(buf, offset, objectList.size() * Long.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    double v = 0;
                    if (o != null) {
                        v = (Double) o;
                    }
                    long l = Double.doubleToLongBits(v);
                    SerializeLong(buf, offset, l);
                    offset += Long.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                SerializeInt(buf, offset, objectList.size() * Long.BYTES);
                offset += Integer.BYTES;

                for (Object o: objectList){
                    if (o != null) {
                        if (o instanceof Instant){
                            Instant instant = (Instant) o;
                            long v = DateTimeUtils.toLong(instant, precision);

                            SerializeLong(buf, offset, v);
                            offset += Long.BYTES;
                        } else if (o instanceof OffsetDateTime){
                            OffsetDateTime offsetDateTime = (OffsetDateTime) o;
                            long v = DateTimeUtils.toLong(offsetDateTime.toInstant(), precision);

                            SerializeLong(buf, offset, v);
                            offset += Long.BYTES;
                        } else if (o instanceof ZonedDateTime){
                            ZonedDateTime zonedDateTime = (ZonedDateTime) o;
                            long v = DateTimeUtils.toLong(zonedDateTime.toInstant(), precision);

                            SerializeLong(buf, offset, v);
                            offset += Long.BYTES;
                        } else if (o instanceof Long){
                            SerializeLong(buf, offset, (Long) o);
                            offset += Long.BYTES;
                        } else {
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported timestamp data type : " + o.getClass().getName());
                        }

                    } else {
                        SerializeLong(buf, offset, 0);
                        offset += Long.BYTES;
                    }
                }
                break;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + dataType);
        }
    }

    private static void SerializeArrayDataType(int dataType , byte[] buf, int offset, List<Object> objectList) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:{
                for (Object o: objectList){
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        serializeByteArray(buf, offset, v);
                        offset += v.length;
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_NCHAR: {
                for (Object o: objectList){
                    if (o != null) {
                        String v = (String) o;
                        byte[] bytes = v.getBytes();
                        serializeByteArray(buf, offset, bytes);
                        offset += bytes.length;
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
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:{
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
            case TSDB_DATA_TYPE_NCHAR: {
                int totalLength = 0;
                for (Object o : column.getDataList()) {
                    if (o != null) {
                        String v = (String) o;
                        totalLength += v.getBytes().length;
                    }
                }
                // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 * v.length + totalLength
                return 17 + (5 * column.getDataList().size()) + totalLength;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + column.getType());
        }
    }

    public static byte[] getStmt2BindBlock(long reqId,
                                           long stmtId,
                                           HashMap<ByteBuffer, TableInfo> tableInfoMap,
                                           int toBeBindTableNameIndex,
                                           int toBebindTagCount,
                                           int toBebindColCount,
                                           int precision) throws IOException, SQLException {

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

        byte[] buf = new byte[58 + totalSize];
        int offset = 0;

        //************ header *****************
        // ReqId
        SerializeLong(buf, offset, reqId);
        offset += Long.BYTES;
        // stmtId
        SerializeLong(buf, offset, stmtId);
        offset += Long.BYTES;
        // actionId
        SerializeLong(buf, offset, 9L);
        offset += Long.BYTES;

        // version
        SerializeShort(buf, offset, (short) 1);
        offset += Short.BYTES;

        // col_idx
        SerializeInt(buf, offset, -1);
        offset += Integer.BYTES;

        //************ data *****************
        // TotalLength
        SerializeInt(buf, offset, totalSize + 28);
        offset += Integer.BYTES;

        // tableCount
        SerializeInt(buf, offset, tableInfoMap.size());
        offset += Integer.BYTES;

        // TagCount
        SerializeInt(buf, offset, toBebindTagCount);
        offset += Integer.BYTES;

        // ColCount
        SerializeInt(buf, offset, toBebindColCount);
        offset += Integer.BYTES;

        // tableNameOffset
        if (toBebindTableNameCount > 0){
            SerializeInt(buf, offset, 0x1C);
            offset += Integer.BYTES;
        } else {
            SerializeInt(buf, offset, 0);
            offset += Integer.BYTES;
        }

        // tagOffset
        if (toBebindTagCount > 0){
            if (toBebindTableNameCount > 0){
                SerializeInt(buf, offset, 28 + totalTableNameSize + Short.BYTES * tableInfoMap.size());
                offset += Integer.BYTES;
            } else {
                SerializeInt(buf, offset, 28);
                offset += Integer.BYTES;
            }
        } else {
            SerializeInt(buf, offset, 0);
            offset += Integer.BYTES;
        }

        // colOffset
        if (toBebindColCount > 0){
            int skipSize = 0;
            if (toBebindTableNameCount > 0){
                skipSize += totalTableNameSize + Short.BYTES * tableInfoMap.size();
            }

            if (toBebindTagCount > 0){
                skipSize += totalTagSize + Integer.BYTES * tableInfoMap.size();
            }
            SerializeInt(buf, offset, 28 + skipSize);
            offset += Integer.BYTES;
        } else {
            SerializeInt(buf, offset, 0);
            offset += Integer.BYTES;
        }

        // TableNameLength
        if (toBebindTableNameCount > 0){
            for (Short tableNameLen: tableNameSizeList){
                if (tableNameLen == 0) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                }

                SerializeShort(buf, offset, tableNameLen);
                offset += Short.BYTES;
            }

            for (TableInfo tableInfo: tableInfoMap.values()){
                if (tableInfo.getTableName().capacity() == 0) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                }

                serializeByteArray(buf, offset, tableInfo.getTableName().array());
                offset += tableInfo.getTableName().capacity();
                buf[offset++] = 0;
            }
        }

        // TagsDataLength
        if (toBebindTagCount > 0){
            for (Integer tagsize: tagSizeList) {
                   SerializeInt(buf, offset, tagsize);
                   offset += Integer.BYTES;
            }

            for (TableInfo tableInfo : tableInfoMap.values()) {
                for (ColumnInfo tag : tableInfo.getTagInfo()){
                    if (tag.getDataList().isEmpty()){
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "tag value is null, tbname: " + tableInfo.getTableName().toString());
                    }
                    serializeColumn(tag, buf, offset, precision);
                    offset += tag.getSerializeSize();
                }
            }
        }

        // ColsDataLength
        if (toBebindColCount > 0){
            for (Integer colSize: colSizeList) {
                SerializeInt(buf, offset, colSize);
                offset += Integer.BYTES;
            }

            for (TableInfo tableInfo : tableInfoMap.values()) {
                for (ColumnInfo col : tableInfo.getDataList()){
                    serializeColumn(col, buf, offset, precision);
                    offset += col.getSerializeSize();
                }
            }
        }


//        for (int i = 30; i < buf.length; i++) {
//            int bb = buf[i] & 0xff;
//            System.out.print(bb);
//            System.out.print(",");
//        }
//        System.out.println();
        return buf;
    }

    // little endian
    public static byte[] shortToBytes(int v) {
        byte[] result = new byte[2];
        result[0] = (byte) (v & 0xFF);
        result[1] = (byte) ((v >> 8) & 0xFF);
        return result;
    }

    // little endian
    public static byte[] intToBytes(int v) {
        byte[] result = new byte[4];
        result[0] = (byte) (v & 0xFF);
        result[1] = (byte) ((v >> 8) & 0xFF);
        result[2] = (byte) ((v >> 16) & 0xFF);
        result[3] = (byte) ((v >> 24) & 0xFF);
        return result;
    }

    // little endian
    public static byte[] longToBytes(long v) {
        byte[] result = new byte[8];
        result[0] = (byte) (v & 0xFF);
        result[1] = (byte) ((v >> 8) & 0xFF);
        result[2] = (byte) ((v >> 16) & 0xFF);
        result[3] = (byte) ((v >> 24) & 0xFF);
        result[4] = (byte) ((v >> 32) & 0xFF);
        result[5] = (byte) ((v >> 40) & 0xFF);
        result[6] = (byte) ((v >> 48) & 0xFF);
        result[7] = (byte) ((v >> 56) & 0xFF);
        return result;
    }
}
