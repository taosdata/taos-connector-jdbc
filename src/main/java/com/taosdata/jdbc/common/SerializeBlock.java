package com.taosdata.jdbc.common;

import com.taosdata.jdbc.enums.TimestampPrecision;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
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

    private static void handleBoolean(byte[] buf, int rowIndex, int offset, Object o){
        boolean v = (Boolean) o;
        if (v) {
            buf[offset + rowIndex] = 1;
        }else {
            buf[offset + rowIndex] = 0;
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

    private static Long getLongFromTimestamp(Timestamp o, int precision){
        long v;
        if (precision == TimestampPrecision.MS) {
            v = o.getTime();
        } else if (precision == TimestampPrecision.US) {
            v = o.getTime() * 1000L + o.getNanos() / 1000 % 1000;
        } else {
            v = o.getTime() * 1000_000L + o.getNanos() % 1000_000L;
        }
        return v;
    }
    private static void handleNormalDataType(int dataType ,byte[] buf, int rowIndex, int startOffset, Object o, int precision) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BOOL: {
                handleBoolean(buf, rowIndex, startOffset, o);
                break;
            }
            case TSDB_DATA_TYPE_TINYINT: {
                buf[rowIndex + startOffset] = (Byte) o;
                break;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
                short v = (Short) o;
                int offset = rowIndex * Short.BYTES + startOffset;
                SerializeShort(buf, offset, v);
                break;
            }
            case TSDB_DATA_TYPE_INT: {
                int v = (Integer) o;
                int offset = rowIndex * Integer.BYTES + startOffset;
                SerializeInt(buf, offset, v);
                break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                long v = (Long) o;
                int offset = rowIndex * Long.BYTES + startOffset;
                SerializeLong(buf, offset, v);
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float v = (Float) o;
                int offset = rowIndex * Float.BYTES + startOffset;
                int f = Float.floatToIntBits(v);
                SerializeInt(buf, offset, f);
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double v = (Double) o;
                int offset = rowIndex * Double.BYTES + startOffset;
                long l = Double.doubleToLongBits(v);
                SerializeLong(buf, offset, l);
                break;
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                long v;
                if (o instanceof Timestamp) {
                    Timestamp t = (Timestamp) o;
                    v = getLongFromTimestamp(t, precision);
                } else if (o instanceof Instant){
                    Instant t = (Instant) o;
                    Timestamp ts = Timestamp.from(t);
                    v = getLongFromTimestamp(ts, precision);
                } else if (o instanceof OffsetDateTime){
                    OffsetDateTime t = (OffsetDateTime) o;
                    Timestamp ts = Timestamp.from(t.toInstant());
                    v = getLongFromTimestamp(ts, precision);
                } else if (o instanceof ZonedDateTime){
                    ZonedDateTime t = (ZonedDateTime) o;
                    Timestamp ts = Timestamp.from(t.toInstant());
                    v = getLongFromTimestamp(ts, precision);
                } else {
                    throw new SQLException("unsupported data type : " + o.getClass().getName());
                }

                int offset = rowIndex * Long.BYTES + startOffset;
                SerializeLong(buf, offset, v);
                break;
            }
            default:
                throw new SQLException("unsupported data type : " + dataType);
        }
    }


    public static byte[] getRawBlock(List<ColumnInfo> list, int precision) throws IOException, SQLException {
        int columns = list.size();
        int rows = list.get(0).getDataList().size();

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // version int32
        buffer.write(intToBytes(1));
        // length int32
        buffer.write(intToBytes(0));
        // rows int32
        buffer.write(intToBytes(rows));
        // columns int32
        buffer.write(intToBytes(columns));
        // flagSegment int32
        buffer.write(intToBytes(0));
        // groupID uint64
        buffer.write(longToBytes(0));

        byte[] colInfoData = new byte[5 * columns];
        byte[] lengthData = new byte[4 * columns];

        int bitMapLen = bitMapLen(rows);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (int colIndex = 0; colIndex < list.size(); colIndex++) {
            ColumnInfo column = list.get(colIndex);

            Integer dataLen = DataLengthCfg.getDataLength(column.getType());

            // 不支持的数据类型
            if (column.getType() == TSDB_DATA_TYPE_UTINYINT
                    || column.getType() == TSDB_DATA_TYPE_USMALLINT
                    || column.getType() == TSDB_DATA_TYPE_UINT
                    || column.getType() == TSDB_DATA_TYPE_UBIGINT
            ) {
                break;
            }

            //非数组类型
            if (dataLen != null){
                colInfoData[colIndex * 5] = (byte) column.getType();
                int typeLen = dataLen;

                byte[] typeBytes = intToBytes(typeLen);
                System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                byte[] array = intToBytes(typeLen * rows);
                System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                byte[] tmp = new byte[bitMapLen + rows * dataLen];
                List<?> rowData = column.getDataList();

                for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                    if (rowData.get(rowIndex) == null) {
                        int charOffset = charOffset(rowIndex);
                        tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                    } else {
                        handleNormalDataType(column.getType(), tmp, rowIndex, bitMapLen, rowData.get(rowIndex), precision);
                    }
                }
                data.write(tmp);
            }else{
                // 数组类型
                switch (column.getType()) {
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    {
                        colInfoData[colIndex * 5] = (byte) column.getType();
                        // 4 bytes for 0

                        int length = 0;
                        List<?> rowData = column.getDataList();
                        byte[] index = new byte[rows * Integer.BYTES];
                        List<Byte> tmp = new ArrayList<>();
                        for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                            int offset = rowIndex * Integer.BYTES;
                            if (rowData.get(rowIndex) == null) {
                                for (int i = 0; i < Integer.BYTES; i++) {
                                    index[offset + i] = (byte) 0xFF;
                                }
                            } else {
                                byte[] v = (byte[]) rowData.get(rowIndex);
                                for (int i = 0; i < Integer.BYTES; i++) {
                                    index[offset + i] = (byte) (length >> (8 * i) & 0xFF);
                                }
                                short len = (short) v.length;
                                tmp.add((byte) (len & 0xFF));
                                tmp.add((byte) ((len >> 8) & 0xFF));
                                for (byte b : v) {
                                    tmp.add(b);
                                }
                                length += v.length + Short.BYTES;
                            }
                        }
                        byte[] array = intToBytes(length);
                        System.arraycopy(array, 0, lengthData, colIndex * 4, 4);
                        data.write(index);
                        byte[] bytes = new byte[tmp.size()];
                        for (int i = 0; i < tmp.size(); i++) {
                            bytes[i] = tmp.get(i);
                        }
                        data.write(bytes);
                        break;
                    }
                    case TSDB_DATA_TYPE_NCHAR: {
                        colInfoData[colIndex * 5] = (byte) column.getType();;
                        // 4 bytes for 0

                        int length = 0;
                        byte[] index = new byte[rows * Integer.BYTES];
                        ByteArrayOutputStream tmp = new ByteArrayOutputStream();
                        List<?> rowData = column.getDataList();
                        for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                            int offset = rowIndex * Integer.BYTES;
                            if (rowData.get(rowIndex) == null) {
                                for (int i = 0; i < Integer.BYTES; i++) {
                                    index[offset + i] = (byte) 0xFF;
                                }
                            } else {
                                String v = (String) rowData.get(rowIndex);
                                for (int i = 0; i < Integer.BYTES; i++) {
                                    index[offset + i] = (byte) ((length >> (8 * i)) & 0xFF);
                                }
                                short len = (short) (v.length() * 4);
                                tmp.write((byte) (len & 0xFF));
                                tmp.write((byte) ((len >> 8) & 0xFF));
                                int[] t = v.codePoints().toArray();
                                for (int i : t) {
                                    tmp.write(intToBytes(i));
                                }
                                length += t.length * 4 + Short.BYTES;
                            }
                        }
                        byte[] array = intToBytes(length);
                        System.arraycopy(array, 0, lengthData, colIndex * 4, 4);
                        data.write(index);
                        data.write(tmp.toByteArray());
                        break;
                    }
                    default:
                        throw new SQLException("unsupported data type : " + column.getType());
                }
            }
        }
        buffer.write(colInfoData);
        buffer.write(lengthData);
        buffer.write(data.toByteArray());
        byte[] block = buffer.toByteArray();
        for (int i = 0; i < Integer.BYTES; i++) {
            block[4 + i] = (byte) (block.length >> (8 * i));
        }
        return block;
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
