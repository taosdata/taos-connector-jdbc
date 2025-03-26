package com.taosdata.jdbc;

import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.DecimalUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.taosdata.jdbc.TSDBConstants.*;

public class BlockData {
    private List<List<Object>> data;

    private int returnCode;
    private boolean isCompleted;
    private int numOfRows;
    private ByteBuffer buffer;
    private List<RestfulResultSet.Field> fields;
    private final Semaphore semaphore;
    private int precision;

    public BlockData(List<List<Object>> data,
                     int returnCode,
                     int numOfRows,
                     ByteBuffer buffer,
                     List<RestfulResultSet.Field> fields,
                     int precision) {
        this.data = data;
        this.returnCode = returnCode;
        this.numOfRows = numOfRows;
        this.buffer = buffer;
        this.fields = fields;
        this.semaphore = new Semaphore(0);
        this.isCompleted = false;
        this.precision = precision;
    }

    public static BlockData getEmptyBlockData(List<RestfulResultSet.Field> fields, int precision) {
        return new BlockData(null, 0, 0, null, fields, precision);
    }

    public void handleData() {

        try {
            int columns = fields.size();
            List<List<Object>> list = new ArrayList<>();
            if (buffer != null) {
                buffer.getInt(); // buffer length
                int pHeader = buffer.position() + 28 + columns * 5;
                buffer.position(buffer.position() + 8);
                this.numOfRows = buffer.getInt();

                buffer.position(pHeader);
                int bitMapOffset = BitmapLen(numOfRows);

                List<Integer> lengths = new ArrayList<>(columns);
                for (int i = 0; i < columns; i++) {
                    lengths.add(buffer.getInt());
                }
                pHeader = buffer.position();
                int length = 0;
                for (int i = 0; i < columns; i++) {
                    List<Object> col = new ArrayList<>(numOfRows);
                    int type = fields.get(i).getTaosType();
                    int scale = fields.get(i).getScale();
                    switch (type) {
                        case TSDB_DATA_TYPE_BOOL:
                        case TSDB_DATA_TYPE_TINYINT:
                        case TSDB_DATA_TYPE_UTINYINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                byte b = buffer.get();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(b);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_SMALLINT:
                        case TSDB_DATA_TYPE_USMALLINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                short s = buffer.getShort();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(s);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_INT:
                        case TSDB_DATA_TYPE_UINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                int in = buffer.getInt();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(in);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_BIGINT:
                        case TSDB_DATA_TYPE_UBIGINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                long l = buffer.getLong();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(l);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_TIMESTAMP: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                long l = buffer.getLong();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    Instant instant = DateTimeUtils.parseTimestampColumnData(l, precision);
                                    col.add(instant);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_FLOAT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                float f = buffer.getFloat();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(f);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_DOUBLE: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                double d = buffer.getDouble();
                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(d);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_BINARY:
                        case TSDB_DATA_TYPE_JSON:
                        case TSDB_DATA_TYPE_VARBINARY:
                        case TSDB_DATA_TYPE_GEOMETRY: {
                            length = numOfRows * 4;
                            List<Integer> offset = new ArrayList<>(numOfRows);
                            for (int m = 0; m < numOfRows; m++) {
                                offset.add(buffer.getInt());
                            }
                            int start = buffer.position();
                            for (int m = 0; m < numOfRows; m++) {
                                if (-1 == offset.get(m)) {
                                    col.add(null);
                                    continue;
                                }
                                buffer.position(start + offset.get(m));
                                int len = buffer.getShort() & 0xFFFF;
                                byte[] tmp = new byte[len];
                                buffer.get(tmp);
                                col.add(tmp);
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_NCHAR: {
                            length = numOfRows * 4;
                            List<Integer> offset = new ArrayList<>(numOfRows);
                            for (int m = 0; m < numOfRows; m++) {
                                offset.add(buffer.getInt());
                            }
                            int start = buffer.position();
                            for (int m = 0; m < numOfRows; m++) {
                                if (-1 == offset.get(m)) {
                                    col.add(null);
                                    continue;
                                }
                                buffer.position(start + offset.get(m));
                                int len = (buffer.getShort() & 0xFFFF) / 4;
                                int[] tmp = new int[len];
                                for (int n = 0; n < len; n++) {
                                    tmp[n] = buffer.getInt();
                                }
                                col.add(tmp);
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_DECIMAL128:
                        case TSDB_DATA_TYPE_DECIMAL64:
                            int dataLen = type == TSDB_DATA_TYPE_DECIMAL128 ? 16 : 8;
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.get(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                byte[] tb = new byte[dataLen];
                                buffer.get(tb);

                                if (isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    BigDecimal t = DecimalUtil.getBigDecimal(tb, scale);
                                    col.add(t);
                                }
                            }
                            break;
                        default:
                            // unknown type, do nothing
                            col.add(null);
                            break;
                    }
                    pHeader += length + lengths.get(i);
                    buffer.position(pHeader);
                    list.add(col);
                }
            }
            this.data = list;
            semaphore.release();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private int BitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    private boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] & (1 << (7 - index))) == (1 << (7 - index));
    }

    public void doneWithNoData(){
        semaphore.release();
    }

    public void waitTillOK() throws SQLException {
        try {
            // must be ok When the CPU has idle time
            if (!semaphore.tryAcquire(50, TimeUnit.SECONDS))
            {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FETCH DATA TIME OUT");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }




    public List<List<Object>> getData() {
        return data;
    }

    public void setData(List<List<Object>> data) {
        this.data = data;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public int getNumOfRows() {
        return numOfRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public List<RestfulResultSet.Field> getFields() {
        return fields;
    }

    public void setFields(List<RestfulResultSet.Field> fields) {
        this.fields = fields;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }
}
