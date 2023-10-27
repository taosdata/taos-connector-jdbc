package com.taosdata.jdbc.ws.tmq;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.FetchBlockResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.tmq.entity.FetchResp;
import com.taosdata.jdbc.ws.tmq.entity.TMQRequestFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;

public class WSConsumerResultSet extends AbstractResultSet {
    private final Transport transport;
    private final TMQRequestFactory factory;
    private final long messageId;
    private final String database;

    protected volatile boolean isClosed;
    // meta
    protected RestfulResultSetMetaData metaData;
    protected final List<RestfulResultSet.Field> fields = new ArrayList<>();
    protected List<String> columnNames;
    // data
    protected List<List<Object>> result = new ArrayList<>();

    protected int numOfRows = 0;
    protected int rowIndex = 0;

    public WSConsumerResultSet(Transport transport, TMQRequestFactory factory, long messageId, String database) {
        this.transport = transport;
        this.factory = factory;
        this.messageId = messageId;
        this.database = database;
    }

    private boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        if (this.forward())
            return true;

        Request request = factory.generateFetch(messageId);
        FetchResp fetchResp = (FetchResp) transport.send(request);
        if (Code.SUCCESS.getCode() != fetchResp.getCode())
            throw TSDBError.createSQLException(fetchResp.getCode(), fetchResp.getMessage());

        this.reset();
        if (fetchResp.isCompleted() || fetchResp.getRows() == 0)
            return false;

        columnNames = Arrays.asList(fetchResp.getFieldsNames());
        fields.clear();
        for (int i = 0; i < fetchResp.getFieldsCount(); i++) {
            String colName = fetchResp.getFieldsNames()[i];
            int taosType = fetchResp.getFieldsTypes()[i];
            int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
            int length = (int) fetchResp.getFieldsLengths()[i];
            fields.add(new RestfulResultSet.Field(colName, jdbcType, length, "", taosType));
        }
        this.metaData = new RestfulResultSetMetaData(database, fields, fetchResp.getTableName());
        this.numOfRows = fetchResp.getRows();
        this.result = fetchBlockData(request.id());
        this.timestampPrecision = fetchResp.getPrecision();
        return true;
    }

    public List<List<Object>> fetchBlockData(long fetchRequestId) throws SQLException {
        Request blockRequest = factory.generateFetchBlock(fetchRequestId, messageId);
        FetchBlockResp resp = (FetchBlockResp) transport.send(blockRequest);
        ByteBuffer buffer = resp.getBuffer();
        List<List<Object>> list = new ArrayList<>();
        if (resp.getBuffer() != null) {
            int bitMapOffset = BitmapLen(numOfRows);
            int pHeader = buffer.position() + 28 + fields.size() * 5;
            buffer.position(pHeader);
            List<Integer> lengths = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                lengths.add(buffer.getInt());
            }
            pHeader = buffer.position();
            int length = 0;
            for (int i = 0; i < fields.size(); i++) {
                List<Object> col = new ArrayList<>(numOfRows);
                int type = fields.get(i).getTaosType();
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
                    case TSDB_DATA_TYPE_VARBINARY: {
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
        return list;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!this.isClosed) {
            this.isClosed = true;
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof String)
            return (String) value;

        if (value instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            try {
                return new String((byte[]) value, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return false;
        }
        wasNull = false;
        if (value instanceof Boolean)
            return (boolean) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_TINYINT:
                return ((byte) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return ((short) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return ((int) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (((long) value) == 0L) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_TIMESTAMP:
                return ((Timestamp) value).getTime() == 0L ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UBIGINT:
                return value.equals(new BigDecimal(0)) ? Boolean.FALSE : Boolean.TRUE;

            case TSDB_DATA_TYPE_FLOAT:
                return (((float) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_DOUBLE: {
                return (((double) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_NCHAR: {
                if ("TRUE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.FALSE;
                } else {
                    throw new SQLDataException();
                }
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                if ("TRUE".compareToIgnoreCase(tmp) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase(tmp) == 0) {
                    return Boolean.FALSE;
                } else {
                    throw new SQLDataException();
                }
            }
        }

        return Boolean.FALSE;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Byte)
            return (byte) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (byte) 1 : (byte) 0;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT: {
                short tmp = (short) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                int tmp = (int) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Byte.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Byte.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);

                return tmp.byteValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Byte.parseByte((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Byte.parseByte(tmp);
            }
        }

        return 0;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + jdbcType2TaosTypeName(jdbcType));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Short)
            return (short) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (short) 1 : (short) 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                int tmp = (int) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Short.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Short.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return tmp.shortValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Short.parseShort((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Short.parseShort(tmp);
            }
        }
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Integer)
            return (int) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Integer.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Integer.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return tmp.intValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Integer.parseInt((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Integer.parseInt(tmp);
            }
        }
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Long)
            return (long) value;
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
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

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Long.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return tmp.longValue();
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                Timestamp ts = (Timestamp) value;
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
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return (long) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (Double) value;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return (long) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Long.parseLong((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Long.parseLong(tmp);
            }
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Float)
            return (float) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (float) 1 : (float) 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Float.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Float.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.FLOAT);
                return tmp.floatValue();
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                Double tmp = (Double) value;
                if (tmp < Float.MIN_VALUE || tmp > Float.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.FLOAT);
                return Float.parseFloat(String.valueOf(tmp));
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Float.parseFloat(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Float.parseFloat(tmp);
            }
        }
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Double)
            return (double) value;
        if (value instanceof Float)
            return Double.parseDouble(String.valueOf(value));

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Double.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Double.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.DOUBLE);
                return tmp.floatValue();
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Double.parseDouble(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Double.parseDouble(tmp);
            }
        }
        return 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof byte[])
            return (byte[]) value;
        if (value instanceof String)
            return ((String) value).getBytes();
        if (value instanceof Long)
            return Longs.toByteArray((long) value);
        if (value instanceof Integer)
            return Ints.toByteArray((int) value);
        if (value instanceof Short)
            return Shorts.toByteArray((short) value);
        if (value instanceof Byte)
            return new byte[]{(byte) value};

        return value.toString().getBytes();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof Long) {
            return parseTimestampColumnData((long) value);
        }
        Timestamp ret;
        try {
            ret = Utils.parseTimestamp(value.toString());
        } catch (Exception e) {
            ret = null;
            wasNull = true;
        }
        return ret;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        wasNull = value == null;
        return value;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in result");
        return columnIndex + 1;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof BigDecimal)
            return (BigDecimal) value;


        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? new BigDecimal(1) : new BigDecimal(0);
            case TSDB_DATA_TYPE_TINYINT:
                return new BigDecimal((byte) value);
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return new BigDecimal((short) value);
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return new BigDecimal((int) value);
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return new BigDecimal((long) value);

            case TSDB_DATA_TYPE_FLOAT:
                return BigDecimal.valueOf((float) value);
            case TSDB_DATA_TYPE_DOUBLE:
                return BigDecimal.valueOf((double) value);

            case TSDB_DATA_TYPE_TIMESTAMP:
                return new BigDecimal(((Timestamp) value).getTime());
            case TSDB_DATA_TYPE_NCHAR:
                return new BigDecimal(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return new BigDecimal(tmp);
            }
        }

        return new BigDecimal(0);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.rowIndex == -1 && this.numOfRows != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.rowIndex >= numOfRows && this.numOfRows != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.rowIndex == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.numOfRows == 0)
            return false;
        return this.rowIndex == (this.numOfRows - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        synchronized (this) {
            if (this.numOfRows > 0) {
                this.rowIndex = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        synchronized (this) {
            if (this.numOfRows > 0) {
                this.rowIndex = this.numOfRows;
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        if (this.numOfRows == 0)
            return false;

        synchronized (this) {
            this.rowIndex = 0;
        }
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.numOfRows == 0)
            return false;
        synchronized (this) {
            this.rowIndex = this.numOfRows - 1;
        }
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        int row;
        synchronized (this) {
            if (this.rowIndex < 0 || this.rowIndex >= this.numOfRows)
                return 0;
            row = this.rowIndex + 1;
        }
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);

    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    private Timestamp parseTimestampColumnData(long value) {
        if (TimestampPrecision.MS == this.timestampPrecision)
            return new Timestamp(value);

        if (TimestampPrecision.US == this.timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        if (TimestampPrecision.NS == this.timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        return null;
    }

    public Object parseValue(int columnIndex) {
        Object source = result.get(columnIndex - 1).get(rowIndex);
        if (null == source)
            return null;

        int type = fields.get(columnIndex - 1).getTaosType();
        switch (type) {
            case TSDB_DATA_TYPE_BOOL: {
                byte val = (byte) source;
                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                byte val = (byte) source;
                return parseUTinyInt(val);
            }
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY: {
                return source;
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
            case TSDB_DATA_TYPE_NCHAR: {
                int[] tmp = (int[]) source;
                return new String(tmp, 0, tmp.length);
            }
            default:
                // unknown type, do nothing
                return null;
        }
    }

    //    ceil(numOfRows/8.0)
    private int BitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    private boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] & (1 << (7 - index))) == (1 << (7 - index));
    }
}
