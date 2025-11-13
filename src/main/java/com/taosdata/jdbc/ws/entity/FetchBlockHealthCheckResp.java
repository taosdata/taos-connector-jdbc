package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.DecimalUtil;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL128;

public class FetchBlockHealthCheckResp extends FetchBlockNewResp {

    private boolean isClusterAlive;
    public FetchBlockHealthCheckResp(ByteBuf buffer) {
        super(buffer);
        super.init();

        if (getCode() == Code.SUCCESS.getCode() && !isCompleted()){
            try {
                int columns = fields.size();
                List<List<Object>> list = new ArrayList<>();
                if (buffer != null) {
                    buffer.readIntLE(); // buffer length
                    int pHeader = buffer.readerIndex() + 28 + columns * 5;
                    buffer.skipBytes(8);
                    this.numOfRows = buffer.readIntLE();

                    buffer.readerIndex(pHeader);
                    int bitMapOffset = bitmapLen(numOfRows);

                    List<Integer> lengths = new ArrayList<>(columns);
                    for (int i = 0; i < columns; i++) {
                        lengths.add(buffer.readIntLE());
                    }
                    pHeader = buffer.readerIndex();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    byte b = buffer.readByte();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    short s = buffer.readShortLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    int in = buffer.readIntLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    long l = buffer.readLongLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    long l = buffer.readLongLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    float f = buffer.readFloatLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    double d = buffer.readDoubleLE();
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
                            case TSDB_DATA_TYPE_BLOB:
                            case TSDB_DATA_TYPE_VARBINARY:
                            case TSDB_DATA_TYPE_GEOMETRY: {
                                length = numOfRows * 4;
                                List<Integer> offset = new ArrayList<>(numOfRows);
                                for (int m = 0; m < numOfRows; m++) {
                                    offset.add(buffer.readIntLE());
                                }
                                int start = buffer.readerIndex();
                                for (int m = 0; m < numOfRows; m++) {
                                    if (-1 == offset.get(m)) {
                                        col.add(null);
                                        continue;
                                    }
                                    buffer.readerIndex(start + offset.get(m));
                                    int len;
                                    if (type != TSDB_DATA_TYPE_BLOB) {
                                        len = buffer.readShortLE() & 0xFFFF;

                                    } else {
                                        len = buffer.readIntLE();
                                    }

                                    byte[] tmp = new byte[len];
                                    buffer.readBytes(tmp);
                                    col.add(tmp);
                                }
                                break;
                            }
                            case TSDB_DATA_TYPE_NCHAR: {
                                length = numOfRows * 4;
                                List<Integer> offset = new ArrayList<>(numOfRows);
                                for (int m = 0; m < numOfRows; m++) {
                                    offset.add(buffer.readIntLE());
                                }
                                int start = buffer.readerIndex();
                                for (int m = 0; m < numOfRows; m++) {
                                    if (-1 == offset.get(m)) {
                                        col.add(null);
                                        continue;
                                    }
                                    buffer.readerIndex(start + offset.get(m));
                                    int len = (buffer.readShortLE() & 0xFFFF) / 4;
                                    int[] tmp = new int[len];
                                    for (int n = 0; n < len; n++) {
                                        tmp[n] = buffer.readIntLE();
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
                                buffer.readBytes(tmp);
                                for (int j = 0; j < numOfRows; j++) {
                                    byte[] tb = new byte[dataLen];
                                    buffer.readBytes(tb);

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
                        buffer.readerIndex(pHeader);
                        list.add(col);
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                ReferenceCountUtil.safeRelease(buffer);
            }
        }
        else {
            isClusterAlive = false;
        }

    }

}
