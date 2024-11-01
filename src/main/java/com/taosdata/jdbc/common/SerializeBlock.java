package com.taosdata.jdbc.common;

import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
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
        for (int i = 0; i < columnInfo.getDataList().size(); i++) {
            if (columnInfo.getDataList().get(i) == null) {
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
        for (Object o: columnInfo.getDataList()){
            if (o == null){
                SerializeInt(buf, offset, 0);
                offset += Integer.BYTES;
            } else {
                switch (columnInfo.getType()) {
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:{
                        byte[] v = (byte[]) o;
                        SerializeInt(buf, offset, v.length);
                        offset += Integer.BYTES;
                        break;
                    }
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_NCHAR: {
                        String v = (String) o;
                        SerializeInt(buf, offset, v.length());
                        offset += Integer.BYTES;
                        break;
                    }
                    default:
                        throw new SQLException("unsupported data type : " + columnInfo.getType());
                }
            }
        }

        // Buffer
        SerializeArrayDataType(columnInfo.getType(), buf, offset, columnInfo.getDataList());
        return offset;
    }

    private static void SerializeNormalDataType(int dataType , byte[] buf, int offset, List<Object> objectList, int precision) throws IOException, SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BOOL: {
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
            case TSDB_DATA_TYPE_SMALLINT: {
                for (Object o: objectList){
                    short v = 0;
                    if (o != null) {
                        v = (Short) o;
                    }
                    SerializeShort(buf, offset, v);
                    offset += Short.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_INT: {
                for (Object o: objectList){
                    int v = 0;
                    if (o != null) {
                        v = (Integer) o;
                    }
                    SerializeInt(buf, offset, v);
                    offset += Integer.BYTES;
                }
                break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                for (Object o: objectList){
                    long v = 0;
                    if (o != null) {
                        v = (Long) o;
                    }
                    SerializeLong(buf, offset, v);
                    offset += Long.BYTES;
                }
               break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
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
                for (Object o: objectList){
                    if (o != null) {
                        Timestamp t = (Timestamp) o;
                        long v;
                        if (precision == TimestampPrecision.MS) {
                            v = t.getTime();
                        } else if (precision == TimestampPrecision.US) {
                            v = t.getTime() * 1000L + t.getNanos() / 1000 % 1000;
                        } else {
                            v = t.getTime() * 1000_000L + t.getNanos() % 1000_000L;
                        }
                        SerializeLong(buf, offset, v);
                        offset += Long.BYTES;
                    } else {
                        SerializeLong(buf, offset, 0);
                        offset += Long.BYTES;
                    }
                }
                break;
            }
            default:
                throw new SQLException("unsupported data type : " + dataType);
        }
    }

    private static void SerializeArrayDataType(int dataType , byte[] buf, int offset, List<Object> objectList) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:{
                for (Object o: objectList){
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        for (byte b : v) {
                            buf[offset++] = b;
                        }
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_NCHAR: {
                for (Object o: objectList){
                    if (o != null) {
                        String v = (String) o;
                        for (byte b : v.getBytes()) {
                            buf[offset++] = b;
                        }
                    }
                }
                break;
            }
            default:
                throw new SQLException("unsupported data type : " + dataType);
        }
    }
    public static int getTableNameTotalLength(List<TableInfo> tableInfoList, int toBebindTableNameCount) throws SQLException{
        int totalLength = 0;
        if (toBebindTableNameCount > 0){
            for ( TableInfo tableInfo: tableInfoList){
                if (StringUtils.isEmpty(tableInfo.getTableName())) {
                    throw new SQLException("table name is empty");
                }
                totalLength += tableInfo.getTableName().length() + 1;
            }
        }
        return totalLength;
    }
    public static int getTagTotalLengthByTableIndex(List<TableInfo> tableInfoList, int index, int toBebindTagCount) throws SQLException{
        int totalLength = 0;
        if (toBebindTagCount > 0){
            if (tableInfoList.get(index).getTagInfo().size() != toBebindTagCount){
                throw new SQLException("table tag size is not match");
            }

            for (ColumnInfo tag : tableInfoList.get(index).getTagInfo()){
                if (tag.getDataList().get(index) == null){
                    throw new SQLException("tag value is null, index: " + index);
                }
                int columnSize = getColumnSize(tag);
                tag.setSerializeSize(columnSize);
                totalLength += columnSize;
            }
        }
        return totalLength;
    }

    public static int getColTotalLengthByTableIndex(List<TableInfo> tableInfoList, int index, int toBebindColCount) throws SQLException{
        int totalLength = 0;
        if (toBebindColCount > 0){
            if (tableInfoList.get(index).getDataList().size() != toBebindColCount){
                throw new SQLException("table column size is not match");
            }

            for (ColumnInfo columnInfo : tableInfoList.get(index).getDataList()){
                if (columnInfo.getDataList().get(index) == null){
                    throw new SQLException("col value is null, index: " + index);
                }
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
                // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 + v.length + totalLength
                return 17 + (5 * column.getDataList().size()) + totalLength;
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_NCHAR: {
                int totalLength = 0;
                for (Object o : column.getDataList()) {
                    if (o != null) {
                        String v = (String) o;
                        totalLength += v.length();
                    }
                }
                // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 + v.length + totalLength
                return 17 + (5 * column.getDataList().size()) + totalLength;
            }
            default:
                throw new SQLException("unsupported data type : " + column.getType());
        }
    }




    public static byte[] getStmt2BindBlock(long reqId,
                                           long stmtId,
                                            List<TableInfo> tableInfoList,
                                           int toBebindTableNameCount,
                                           int toBebindTagCount,
                                           int toBebindColCount,
                                           int precision) throws IOException, SQLException {

        // cloc totol size
        int totalTableNameSize  = 0;
        List<Short> tableNameSizeList = new ArrayList<>();
        for (int i = 0; i < tableInfoList.size(); i++) {
            int tableNameSize = getTableNameTotalLength(tableInfoList, toBebindTableNameCount);
            totalTableNameSize += tableNameSize;
            tableNameSizeList.add((short)tableNameSize);
        }

        int totalagSize = 0;
        List<Integer> tagSizeList = new ArrayList<>();

        for (int i = 0; i < tableInfoList.size(); i++) {
            int tagSize = getTagTotalLengthByTableIndex(tableInfoList, i, toBebindTagCount);
            totalagSize += tagSize;
            tagSizeList.add(tagSize);
        }

        int totalColSize = 0;
        List<Integer> colSizeList = new ArrayList<>();
        for (int i = 0; i < tableInfoList.size(); i++) {
            int colSize = getColTotalLengthByTableIndex(tableInfoList, i, toBebindColCount);
            totalColSize += colSize;
            colSizeList.add(colSize);
        }

        int totalSize = totalTableNameSize + totalagSize + totalColSize;
        totalSize += tagSizeList.size() * (toBebindTableNameCount * Short.BYTES + toBebindTagCount * Integer.BYTES  + toBebindColCount * Integer.BYTES);

        byte[] buf = new byte[30 + totalSize];
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
        SerializeInt(buf, offset, totalSize);
        offset += Integer.BYTES;

        // tableCount
        SerializeInt(buf, offset, tableInfoList.size());
        offset += Integer.BYTES;

        // TagCount
        SerializeInt(buf, offset, toBebindTagCount);
        offset += Integer.BYTES;

        // ColCount
        SerializeInt(buf, offset, toBebindColCount);
        offset += Integer.BYTES;

        // tableNameOffset
        int totalTableNameOffset = 0;
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
                SerializeInt(buf, offset, 28 + totalTableNameSize + Short.BYTES * tableInfoList.size());
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
                skipSize += totalTableNameSize + Short.BYTES * tableInfoList.size();
            }

            if (toBebindTagCount > 0){
                skipSize += totalagSize + Integer.BYTES * tableInfoList.size();
            }
            SerializeInt(buf, offset, 28 + skipSize);
            offset += Integer.BYTES;
        } else {
            SerializeInt(buf, offset, 0);
            offset += Integer.BYTES;
        }

        // TableNameLength
        if (toBebindTableNameCount > 0){
            for (Short tabeNameLen: tableNameSizeList){
                if (tabeNameLen == 0) {
                    throw new SQLException("table name is empty");
                }

                SerializeShort(buf, offset, (short)(tabeNameLen + 1));
                offset += Short.BYTES;
            }

            for (TableInfo tableInfo: tableInfoList){
                if (StringUtils.isEmpty(tableInfo.getTableName())) {
                    throw new SQLException("table name is empty");
                }

                serializeByteArray(buf, offset, tableInfo.getTableName().getBytes());
                offset += tableInfo.getTableName().length();
                buf[offset++] = 0;
            }
        }

        // TagsDataLength
        if (toBebindTagCount > 0){
            for (Integer tagsize: tagSizeList) {
                   SerializeInt(buf, offset, tagsize);
                     offset += Integer.BYTES;
            }

            for (int i = 0; i < tableInfoList.size(); i++) {
                for (ColumnInfo tag : tableInfoList.get(i).getTagInfo()){
                    if (tag.getDataList().get(i) == null){
                        throw new SQLException("tag value is null, index: " + i);
                    }
                    serializeColumn(tag, buf, offset, precision);
                    offset += tag.getSerializeSize();
                }
            }
        }

        // TagsDataLength
        if (toBebindColCount > 0){
            for (Integer colSize: colSizeList) {
                SerializeInt(buf, offset, colSize);
                offset += Integer.BYTES;
            }

            for (int i = 0; i < tableInfoList.size(); i++) {
                for (ColumnInfo col : tableInfoList.get(i).getDataList()){
                    if (col.getDataList().get(i) == null){
                        throw new SQLException("tag value is null, index: " + i);
                    }
                    serializeColumn(col, buf, offset, precision);
                    offset += col.getSerializeSize();
                }
            }
        }

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
