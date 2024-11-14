package com.taosdata.jdbc.ws.tmq;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.utils.DataTypeConverUtil;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.tmq.entity.FetchRawBlockResp;
import com.taosdata.jdbc.ws.tmq.entity.TMQRequestFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
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
        if (this.numOfRows > 0){
            return false;
        }

        Request request = factory.generateFetchRaw(messageId);
        FetchRawBlockResp fetchResp = (FetchRawBlockResp) transport.send(request);
        fetchResp.init();

        if (Code.SUCCESS.getCode() != fetchResp.getCode())
            throw TSDBError.createSQLException(fetchResp.getCode(), fetchResp.getMessage());

        fetchResp.parseBlockInfos();

        this.reset();
        if (fetchResp.getRows() == 0)
            return false;

        columnNames = fetchResp.getColumnNames();
        fields.clear();
        fields.addAll(fetchResp.getFields());

        this.metaData = new RestfulResultSetMetaData(database, fields, fetchResp.getTableName());
        this.numOfRows = fetchResp.getRows();
        this.result = fetchResp.getResultData();
        this.timestampPrecision = fetchResp.getPrecision();
        return true;
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
        return DataTypeConverUtil.getBoolean(taosType, value);
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
        return DataTypeConverUtil.getByte(taosType, value, columnIndex);
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
        return DataTypeConverUtil.getShort(taosType, value, columnIndex);
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
        return DataTypeConverUtil.getInt(taosType, value, columnIndex);
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
        int taosType = fields.get(columnIndex - 1).getTaosType();

        return DataTypeConverUtil.getLong(taosType, value, columnIndex, this.timestampPrecision);
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
        return DataTypeConverUtil.getFloat(taosType, value, columnIndex);
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

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConverUtil.getDouble(taosType, value, columnIndex);
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
        return DataTypeConverUtil.getBytes(value);
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
            return DataTypeConverUtil.parseTimestampColumnData((long) value, this.timestampPrecision);
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

        return DataTypeConverUtil.getBigDecimal(taosType, value);
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

    public Object parseValue(int columnIndex) {
        Object source = result.get(columnIndex - 1).get(rowIndex);
        if (null == source)
            return null;

        int type = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConverUtil.parseValue(type, source, this.timestampPrecision);
    }

    //    ceil(numOfRows/8.0)

}
