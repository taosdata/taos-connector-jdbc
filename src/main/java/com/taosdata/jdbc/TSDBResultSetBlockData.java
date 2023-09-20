/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.Utils;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;

public class TSDBResultSetBlockData {
    private int numOfRows = 0;
    private int rowIndex = 0;

    private List<ColumnMetaData> columnMetaDataList;
    private ArrayList<List<Object>> colData;
    public boolean wasNull;

    private int timestampPrecision;

    public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int numOfCols, int timestampPrecision) {
        this.columnMetaDataList = colMeta;
        this.colData = new ArrayList<>(numOfCols);
        this.timestampPrecision = timestampPrecision;
    }

    public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int timestampPrecision) {
        this.columnMetaDataList = colMeta;
        this.colData = new ArrayList<>();
        this.timestampPrecision = timestampPrecision;
    }

    public TSDBResultSetBlockData() {
        this.colData = new ArrayList<>();
    }

    public void clear() {
        int size = this.colData.size();
        this.colData.clear();
        setNumOfCols(size);
    }

    public int getNumOfRows() {
        return this.numOfRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    public int getNumOfCols() {
        return this.colData.size();
    }

    public void setNumOfCols(int numOfCols) {
        this.colData = new ArrayList<>(numOfCols);
    }

    public boolean hasMore() {
        return this.rowIndex < this.numOfRows;
    }

    public boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
    }

    public void setByteArray(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        int bitMapOffset = BitmapLen(numOfRows);
        int pHeader = 28 + columnMetaDataList.size() * 5;
        buffer.position(pHeader);
        List<Integer> lengths = new ArrayList<>(columnMetaDataList.size());
        for (int i = 0; i < columnMetaDataList.size(); i++) {
            lengths.add(buffer.getInt());
        }
        pHeader = buffer.position();
        int length = 0;
        for (int i = 0; i < columnMetaDataList.size(); i++) {
            List<Object> col = new ArrayList<>(numOfRows);
            int type = columnMetaDataList.get(i).getColType();
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
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
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
                case TSDB_DATA_TYPE_GEOMETRY:{
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
                        col.add(new String(tmp, 0, tmp.length));
                    }
                    break;
                }
                default:
                    // unknown type, do nothing
                    col.add(null);
                    break;
            }
            pHeader += length + lengths.get(i);
            buffer.position(pHeader);
            colData.add(col);
        }
    }

    /**
     * The original type may not be a string type, but will be converted to by
     * calling this method
     */
    public String getString(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (obj instanceof String)
            return (String) obj;

        if (obj instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            try {
                return new String((byte[]) obj, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return obj.toString();
    }

    public byte[] getBytes(int col) throws SQLException {

        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (obj instanceof byte[])
            return (byte[]) obj;
        if (obj instanceof String)
            return ((String) obj).getBytes();
        if (obj instanceof Long)
            return Longs.toByteArray((long) obj);
        if (obj instanceof Integer)
            return Ints.toByteArray((int) obj);
        if (obj instanceof Short)
            return Shorts.toByteArray((short) obj);
        if (obj instanceof Byte)
            return new byte[]{(byte) obj};

        return obj.toString().getBytes();
    }

    public int getInt(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;

            case TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return ((Long) obj).intValue();
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) obj;
                if (tmp.compareTo(new BigDecimal(Integer.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Integer.MAX_VALUE)) > 0)
                    throwRangeException(obj.toString(), col, Types.INTEGER);
                return tmp.intValue();
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                return ((Long) ((Timestamp) obj).getTime()).intValue();
            }

            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE: {
                return ((Double) obj).intValue();
            }

            case TSDB_DATA_TYPE_NCHAR: {
                return Integer.parseInt((String) obj);
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                try {
                    return Integer.parseInt(new String((byte[]) obj, charset));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        return 0;
    }

    public boolean getBoolean(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return Boolean.FALSE;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) obj;
            case TSDB_DATA_TYPE_TINYINT:
                return ((byte) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return ((short) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                return ((int) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (((long) obj) == 0L) ? Boolean.FALSE : Boolean.TRUE;

            case TSDB_DATA_TYPE_TIMESTAMP: {
                return ((Timestamp) obj).getTime() == 0L ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDB_DATA_TYPE_UBIGINT:
                return obj.equals(new BigDecimal(0)) ? Boolean.FALSE : Boolean.TRUE;

            case TSDB_DATA_TYPE_FLOAT:
                return (((float) obj) == 0f) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_DOUBLE: {
                return (((double) obj) == 0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_NCHAR: {
                if ("TRUE".compareToIgnoreCase((String) obj) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase((String) obj) == 0) {
                    return Boolean.FALSE;
                } else {
                    throw new SQLDataException();
                }
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                try {
                    String tmp = new String((byte[]) obj, charset);
                    return "TRUE".compareToIgnoreCase(tmp) == 0;
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        return Boolean.FALSE;
    }

    public long getLong(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) obj;
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) obj;
                if (tmp.compareTo(new BigDecimal(Long.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0)
                    throwRangeException(obj.toString(), col, Types.BIGINT);
                return tmp.longValue();
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                Timestamp ts = (Timestamp) obj;
                switch (this.timestampPrecision) {
                    case TimestampPrecision.MS:
                    default:
                        return ts.getTime();
                    case TimestampPrecision.US:
                        return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                    case TimestampPrecision.NS:
                        return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
                }
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) obj;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(obj.toString(), col, Types.BIGINT);
                return (long) tmp;
            }
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (Double) obj;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(obj.toString(), col, Types.BIGINT);
                return (long) tmp;
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                return Long.parseLong((String) obj);
            }
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                try {
                    return Long.parseLong(new String((byte[]) obj, charset));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        return 0;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + jdbcType2TaosTypeName(jdbcType));
    }

    public Timestamp getTimestamp(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        if (type == TSDB_DATA_TYPE_BIGINT)
            return parseTimestampColumnData((long) obj);
        if (type == TSDB_DATA_TYPE_TIMESTAMP)
            return (Timestamp) obj;
        if (obj instanceof byte[]) {
            String tmp = "";
            String charset = TaosGlobalConfig.getCharset();
            try {
                tmp = new String((byte[]) obj, charset);
                return Utils.parseTimestamp(tmp);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return new Timestamp(getLong(col));
    }

    public double getDouble(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDB_DATA_TYPE_UINT:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (long) obj;
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) obj;
                if (tmp.compareTo(BigDecimal.valueOf(Double.MIN_VALUE)) < 0 || tmp.compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) > 0)
                    throwRangeException(obj.toString(), col, Types.TIMESTAMP);
                return tmp.floatValue();
            }
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                Timestamp ts = (Timestamp) obj;
                switch (this.timestampPrecision) {
                    case TimestampPrecision.MS:
                    default:
                        return ts.getTime();
                    case TimestampPrecision.US:
                        return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                    case TimestampPrecision.NS:
                        return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
                }
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return Double.parseDouble(String.valueOf(obj));
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                return (double) obj;
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                return Double.parseDouble((String) obj);
            }
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                try {
                    return Double.parseDouble(new String((byte[]) obj, charset));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        return 0;
    }

    public Object get(int col) {
        List<Object> bb = this.colData.get(col);

        Object source = bb.get(this.rowIndex);
        if (null == source) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        switch (this.columnMetaDataList.get(col).getColType()) {
            case TSDB_DATA_TYPE_BOOL: {
                byte val = (byte) source;
                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:{
                return source;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                byte val = (byte) source;
                return parseUTinyInt(val);
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                short val = (short) source;
                return parseUSmallInt(val);
            }
            case TSDB_DATA_TYPE_UINT: {
                int val = (int) source;
                return parseUInteger(val);
            }

            case TSDB_DATA_TYPE_TIMESTAMP: {
                long val = (long) source;

                return parseTimestampColumnData(val);
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                long val = (long) source;
                return parseUBigInt(val);
            }
            default:
                // unknown type, do nothing
                return null;
        }
    }

    private Timestamp parseTimestampColumnData(long value) {
        if (TimestampPrecision.MS == timestampPrecision)
            return new Timestamp(value);

        if (TimestampPrecision.US == timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        if (TimestampPrecision.NS == timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        return null;
    }

    // ceil(numOfRows/8.0)
    private int BitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    private boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] & (1 << (7 - index))) == (1 << (7 - index));
    }
}
