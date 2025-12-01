package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public abstract class AbstractResultSet extends WrapperImpl implements ResultSet {
    protected final int START_BACKEND_FETCH_BLOCK_NUM = 3;
    private int fetchSize;
    protected boolean wasNull;
    protected int timestampPrecision;

    public void setTimestampPrecision(int timestampPrecision) {
        this.timestampPrecision = timestampPrecision;
    }

    protected void checkAvailability(int columnIndex, int bounds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex < 1)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " < 1");
        if (columnIndex > bounds)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + bounds);
    }


    protected ForkJoinPool getForkJoinPool() {
        return Utils.getForkJoinPool();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        return timestamp == null ? null : new Date(timestamp.getTime());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        return timestamp == null ? null : new Time(timestamp.getTime());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

    }

    @Override
    public String getCursorName() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        //nothing to do
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (rows < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        //nothing to do
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return this.fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void insertRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void deleteRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void refreshRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
       return notSupportedMethod();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        notSupportedMethod();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }


    @Override
    public boolean isAfterLast() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean isLast() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public void beforeFirst() throws SQLException {
        notSupportedMethod();
    }

    @Override
    public void afterLast() throws SQLException {
         notSupportedMethod();
    }

    @Override
    public boolean first() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean last() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public int getRow() throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return notSupportedMethod();
    }

    @Override
    public boolean previous() throws SQLException {
        return notSupportedMethod();
    }

    protected <T> T notSupportedMethod() throws SQLException {
        checkClosed();
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    private void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        }
    }
}
