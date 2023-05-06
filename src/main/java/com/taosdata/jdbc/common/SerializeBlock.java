package com.taosdata.jdbc.common;

import com.taosdata.jdbc.enums.DataLength;
import com.taosdata.jdbc.enums.TimestampPrecision;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        return (byte) (c & ~(1 << (7 - bitPos(n))));
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
            switch (column.getType()) {
                case TSDB_DATA_TYPE_BOOL: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_BOOL;
                    int typeLen = DataLength.TSDB_DATA_TYPE_BOOL.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            boolean v = (Boolean) rowData.get(rowIndex);
                            if (v) {
                                tmp[bitMapLen + rowIndex] = 1;
                            }
                        }
                    }
                    data.write(tmp);
                    break;
                }

                case TSDB_DATA_TYPE_TINYINT: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_TINYINT;
                    int typeLen = DataLength.TSDB_DATA_TYPE_TINYINT.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            tmp[rowIndex + bitMapLen] = (Byte) rowData.get(rowIndex);
                        }
                    }
                    data.write(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_SMALLINT;
                    int typeLen = DataLength.TSDB_DATA_TYPE_SMALLINT.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Short.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            short v = (Short) rowData.get(rowIndex);
                            int offset = rowIndex * Short.BYTES + bitMapLen;
                            tmp[offset] = (byte) (v & 0xFF);
                            tmp[offset + 1] = (byte) ((v >> 8) & 0xFF);
                        }
                    }
                    data.write(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_INT;
                    int typeLen = DataLength.TSDB_DATA_TYPE_INT.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Integer.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {

                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            int v = (Integer) rowData.get(rowIndex);
                            int offset = rowIndex * Integer.BYTES + bitMapLen;
                            tmp[offset] = (byte) (v & 0xFF);
                            tmp[offset + 1] = (byte) ((v >> 8) & 0xFF);
                            tmp[offset + 2] = (byte) ((v >> 16) & 0xFF);
                            tmp[offset + 3] = (byte) ((v >> 24) & 0xFF);
                        }
                    }
                    data.write(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_BIGINT;
                    int typeLen = DataLength.TSDB_DATA_TYPE_BIGINT.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Long.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {

                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            long v = (Long) rowData.get(rowIndex);
                            int offset = rowIndex * Long.BYTES + bitMapLen;
                            tmp[offset] = (byte) (v & 0xFF);
                            tmp[offset + 1] = (byte) ((v >> 8) & 0xFF);
                            tmp[offset + 2] = (byte) ((v >> 16) & 0xFF);
                            tmp[offset + 3] = (byte) ((v >> 24) & 0xFF);
                            tmp[offset + 4] = (byte) ((v >> 32) & 0xFF);
                            tmp[offset + 5] = (byte) ((v >> 40) & 0xFF);
                            tmp[offset + 6] = (byte) ((v >> 48) & 0xFF);
                            tmp[offset + 7] = (byte) ((v >> 56) & 0xFF);

                        }
                    }
                    data.write(tmp);
                    break;
                }

                case TSDB_DATA_TYPE_UTINYINT:
                case TSDB_DATA_TYPE_USMALLINT:
                case TSDB_DATA_TYPE_UINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    break;

                case TSDB_DATA_TYPE_FLOAT: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_FLOAT;
                    int typeLen = DataLength.TSDB_DATA_TYPE_FLOAT.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Float.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            float v = (Float) rowData.get(rowIndex);
                            int offset = rowIndex * Float.BYTES + bitMapLen;
                            int f = Float.floatToIntBits(v);
                            tmp[offset] = (byte) (f & 0xFF);
                            tmp[offset + 1] = (byte) ((f >> 8) & 0xFF);
                            tmp[offset + 2] = (byte) ((f >> 16) & 0xFF);
                            tmp[offset + 3] = (byte) ((f >> 24) & 0xFF);

                        }
                    }
                    data.write(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_DOUBLE;
                    int typeLen = DataLength.TSDB_DATA_TYPE_DOUBLE.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Double.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            double v = (Double) rowData.get(rowIndex);
                            int offset = rowIndex * Double.BYTES + bitMapLen;
                            long l = Double.doubleToLongBits(v);
                            tmp[offset] = (byte) (l & 0xFF);
                            tmp[offset + 1] = (byte) ((l >> 8) & 0xFF);
                            tmp[offset + 2] = (byte) ((l >> 16) & 0xFF);
                            tmp[offset + 3] = (byte) ((l >> 24) & 0xFF);
                            tmp[offset + 4] = (byte) ((l >> 32) & 0xFF);
                            tmp[offset + 5] = (byte) ((l >> 40) & 0xFF);
                            tmp[offset + 6] = (byte) ((l >> 48) & 0xFF);
                            tmp[offset + 7] = (byte) ((l >> 56) & 0xFF);
                        }
                    }
                    data.write(tmp);
                    break;
                }

                case TSDB_DATA_TYPE_BINARY: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_BINARY;
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
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_NCHAR;
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

                case TSDB_DATA_TYPE_TIMESTAMP: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_TIMESTAMP;
                    int typeLen = DataLength.TSDB_DATA_TYPE_TIMESTAMP.getLength();
                    byte[] typeBytes = intToBytes(typeLen);
                    System.arraycopy(typeBytes, 0, colInfoData, colIndex * 5 + 1, 4);
                    byte[] array = intToBytes(typeLen * rows);
                    System.arraycopy(array, 0, lengthData, colIndex * 4, 4);

                    byte[] tmp = new byte[bitMapLen + rows * Long.BYTES];
                    List<?> rowData = column.getDataList();
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
                        if (rowData.get(rowIndex) == null) {
                            int charOffset = charOffset(rowIndex);
                            tmp[charOffset] = bmSetNull(tmp[charOffset], rowIndex);
                        } else {
                            Timestamp t = (Timestamp) rowData.get(rowIndex);
                            long v;
                            if (precision == TimestampPrecision.MS) {
                                v = t.getTime();
                            } else if (precision == TimestampPrecision.US) {
                                v = t.getTime() * 1000L + t.getNanos() / 1000 % 1000_000L;
                            } else {
                                v = t.getTime() * 1000_000L + t.getNanos() % 1000_000L;
                            }

                            int offset = rowIndex * Long.BYTES + bitMapLen;
                            tmp[offset] = (byte) (v & 0xFF);
                            tmp[offset + 1] = (byte) ((v >> 8) & 0xFF);
                            tmp[offset + 2] = (byte) ((v >> 16) & 0xFF);
                            tmp[offset + 3] = (byte) ((v >> 24) & 0xFF);
                            tmp[offset + 4] = (byte) ((v >> 32) & 0xFF);
                            tmp[offset + 5] = (byte) ((v >> 40) & 0xFF);
                            tmp[offset + 6] = (byte) ((v >> 48) & 0xFF);
                            tmp[offset + 7] = (byte) ((v >> 56) & 0xFF);

                        }
                    }
                    data.write(tmp);
                    break;
                }

                case TSDB_DATA_TYPE_JSON: {
                    colInfoData[colIndex * 5] = TSDB_DATA_TYPE_JSON;
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
                default:
                    throw new SQLException("unsupported data type : " + column.getType());
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
