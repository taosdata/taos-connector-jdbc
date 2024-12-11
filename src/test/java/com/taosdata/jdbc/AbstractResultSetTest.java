// src/test/java/com/taosdata/jdbc/AbstractResultSetTest.java
package com.taosdata.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class AbstractResultSetTest {

    private ResultSetTest resultSet = new ResultSetTest();

    @Before
    public void setUp() throws SQLException {

    }

    private  class ResultSetTest extends AbstractResultSet {
        public boolean closed = false;


        @Override
        public boolean next() throws SQLException {
            return false;
        }

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public String getString(int columnIndex) throws SQLException {
            return null;
        }

        @Override
        public boolean getBoolean(int columnIndex) throws SQLException {
            return false;
        }

        @Override
        public byte getByte(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public short getShort(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public int getInt(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public long getLong(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public float getFloat(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public double getDouble(int columnIndex) throws SQLException {
            return 0;
        }

        @Override
        public byte[] getBytes(int columnIndex) throws SQLException {
            return new byte[0];
        }

        @Override
        public Timestamp getTimestamp(int columnIndex) throws SQLException {
            return null;
        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override
        public Object getObject(int columnIndex) throws SQLException {
            return null;
        }

        @Override
        public int findColumn(String columnLabel) throws SQLException {
            return 0;
        }

        @Override
        public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
            return null;
        }

        @Override
        public boolean isBeforeFirst() throws SQLException {
            return false;
        }

        @Override
        public boolean isAfterLast() throws SQLException {
            return false;
        }

        @Override
        public boolean isFirst() throws SQLException {
            return false;
        }

        @Override
        public boolean isLast() throws SQLException {
            return false;
        }

        @Override
        public void beforeFirst() throws SQLException {

        }

        @Override
        public void afterLast() throws SQLException {

        }

        @Override
        public boolean first() throws SQLException {
            return false;
        }

        @Override
        public boolean last() throws SQLException {
            return false;
        }

        @Override
        public int getRow() throws SQLException {
            return 0;
        }

        @Override
        public boolean absolute(int row) throws SQLException {
            return false;
        }

        @Override
        public boolean relative(int rows) throws SQLException {
            return false;
        }

        @Override
        public boolean previous() throws SQLException {
            return false;
        }

        @Override
        public Statement getStatement() throws SQLException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
            return null;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public String getNString(int columnIndex) throws SQLException {
            return null;
        }
    };

    @Test(expected = SQLException.class)
    public void testUpdateNull() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNull(1);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNull2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNull(1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBoolean() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBoolean(1, true);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBoolean2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBoolean(1, true);
    }

    @Test(expected = SQLException.class)
    public void testUpdateByte() throws SQLException {
        resultSet.closed = false;
        resultSet.updateByte(1, (byte) 1);
    }
    @Test(expected = SQLException.class)
    public void testUpdateByte2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateByte(1, (byte) 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateShort() throws SQLException {
        resultSet.closed = false;
        resultSet.updateShort(1, (short) 1);
    }
    @Test(expected = SQLException.class)
    public void testUpdateShort2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateShort(1, (short) 1);
    }


    @Test(expected = SQLException.class)
    public void testUpdateInt() throws SQLException {
        resultSet.closed = false;
        resultSet.updateInt(1, 1);
    }
    @Test(expected = SQLException.class)
    public void testUpdateInt2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateInt(1, 1);
    }


    @Test(expected = SQLException.class)
    public void testUpdateLong() throws SQLException {
        resultSet.closed = false;
        resultSet.updateLong(1, 1L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateLong2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateLong(1, 1L);
    }


    @Test(expected = SQLException.class)
    public void testUpdateFloat() throws SQLException {
        resultSet.closed = false;
        resultSet.updateFloat(1, 1.0f);
    }
    @Test(expected = SQLException.class)
    public void testUpdateFloat2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateFloat(1, 1.0f);
    }

    @Test(expected = SQLException.class)
    public void testUpdateDouble() throws SQLException {
        resultSet.closed = false;
        resultSet.updateDouble(1, 1.0);
    }
    @Test(expected = SQLException.class)
    public void testUpdateDouble2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateDouble(1, 1.0);
    }


    @Test(expected = SQLException.class)
    public void testUpdateBigDecimal() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBigDecimal(1, BigDecimal.ONE);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBigDecimal2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBigDecimal(1, BigDecimal.ONE);
    }


    @Test(expected = SQLException.class)
    public void testUpdateString() throws SQLException {
        resultSet.closed = false;
        resultSet.updateString(1, "value");
    }
    @Test(expected = SQLException.class)
    public void testUpdateString2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateString(1, "value");
    }

    @Test(expected = SQLException.class)
    public void testUpdateBytes() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBytes(1, new byte[]{1, 2, 3});
    }
    @Test(expected = SQLException.class)
    public void testUpdateBytes2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBytes(1, new byte[]{1, 2, 3});
    }


    @Test(expected = SQLException.class)
    public void testUpdateDate() throws SQLException {
        resultSet.closed = false;
        resultSet.updateDate(1, new Date(System.currentTimeMillis()));
    }
    @Test(expected = SQLException.class)
    public void testUpdateDate2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateDate(1, new Date(System.currentTimeMillis()));
    }


    @Test(expected = SQLException.class)
    public void testUpdateTime() throws SQLException {
        resultSet.closed = false;
        resultSet.updateTime(1, new Time(System.currentTimeMillis()));
    }
    @Test(expected = SQLException.class)
    public void testUpdateTime2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateTime(1, new Time(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateTimestamp() throws SQLException {
        resultSet.closed = false;
        resultSet.updateTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }
    @Test(expected = SQLException.class)
    public void testUpdateTimestamp2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }


    @Test(expected = SQLException.class)
    public void testUpdateAsciiStream() throws SQLException {
        resultSet.closed = false;
        resultSet.updateAsciiStream(1, (InputStream) null, 10);
    }
    @Test(expected = SQLException.class)
    public void testUpdateAsciiStream2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateAsciiStream(1, (InputStream) null, 10);
    }



    @Test(expected = SQLException.class)
    public void testUpdateBinaryStream() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBinaryStream(1, (InputStream) null, 10);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBinaryStream2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBinaryStream(1, (InputStream) null, 10);
    }


    @Test(expected = SQLException.class)
    public void testUpdateCharacterStream() throws SQLException {
        resultSet.closed = false;
        resultSet.updateCharacterStream(1, (Reader) null, 10);
    }
    @Test(expected = SQLException.class)
    public void testUpdateCharacterStream2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateCharacterStream(1, (Reader) null, 10);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObjectWithScale() throws SQLException {
        resultSet.closed = false;
        resultSet.updateObject(1, new Object(), 10);
    }
    @Test(expected = SQLException.class)
    public void testUpdateObjectWithScale2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateObject(1, new Object(), 10);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObject() throws SQLException {
        resultSet.closed = false;
        resultSet.updateObject(1, new Object());
    }
    @Test(expected = SQLException.class)
    public void testUpdateObject2() throws SQLException {
        resultSet.closed = false;
        resultSet.updateObject(1, new Object());
    }

    @Test(expected = SQLException.class)
    public void testInsertRow() throws SQLException {
        resultSet.closed = false;
        resultSet.insertRow();
    }
    @Test(expected = SQLException.class)
    public void testInsertRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.insertRow();
    }

    @Test(expected = SQLException.class)
    public void testUpdateRow() throws SQLException {
        resultSet.closed = false;
        resultSet.updateRow();
    }
    @Test(expected = SQLException.class)
    public void testUpdateRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateRow();
    }

    @Test(expected = SQLException.class)
    public void testDeleteRow() throws SQLException {
        resultSet.closed = false;
        resultSet.deleteRow();
    }
    @Test(expected = SQLException.class)
    public void testDeleteRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.deleteRow();
    }

    @Test(expected = SQLException.class)
    public void testRefreshRow() throws SQLException {
        resultSet.closed = false;
        resultSet.refreshRow();
    }
    @Test(expected = SQLException.class)
    public void testRefreshRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.refreshRow();
    }

    @Test(expected = SQLException.class)
    public void testCancelRowUpdates() throws SQLException {
        resultSet.closed = true;
        resultSet.cancelRowUpdates();
    }
    @Test(expected = SQLException.class)
    public void testCancelRowUpdates2() throws SQLException {
        resultSet.closed = false;
        resultSet.cancelRowUpdates();
    }

    @Test(expected = SQLException.class)
    public void testMoveToInsertRow() throws SQLException {
        resultSet.closed = false;
        resultSet.moveToInsertRow();
    }
    @Test(expected = SQLException.class)
    public void testMoveToInsertRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.moveToInsertRow();
    }

    @Test(expected = SQLException.class)
    public void testMoveToCurrentRow() throws SQLException {
        resultSet.closed = false;
        resultSet.moveToCurrentRow();
    }
    @Test(expected = SQLException.class)
    public void testMoveToCurrentRow2() throws SQLException {
        resultSet.closed = true;
        resultSet.moveToCurrentRow();
    }

    @Test(expected = SQLException.class)
    public void testGetObjectWithMap() throws SQLException {
        resultSet.closed = false;
        resultSet.getObject(1, new HashMap<>());
    }
    @Test(expected = SQLException.class)
    public void testGetObjectWithMap2() throws SQLException {
        resultSet.closed = true;
        resultSet.getObject(1, new HashMap<>());
    }

    @Test(expected = SQLException.class)
    public void testGetDateWithCalendar() throws SQLException {
        resultSet.closed = false;
        resultSet.getDate(1, Calendar.getInstance());
    }
    @Test(expected = SQLException.class)
    public void testGetDateWithCalendar2() throws SQLException {
        resultSet.closed = true;
        resultSet.getDate(1, Calendar.getInstance());
    }

    @Test(expected = SQLException.class)
    public void testGetTimeWithCalendar() throws SQLException {
        resultSet.closed = false;
        resultSet.getTime(1, Calendar.getInstance());
    }
    @Test(expected = SQLException.class)
    public void testGetTimeWithCalendar2() throws SQLException {
        resultSet.closed = true;
        resultSet.getTime(1, Calendar.getInstance());
    }

    @Test(expected = SQLException.class)
    public void testGetURLByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.getURL(1);
    }
    @Test(expected = SQLException.class)
    public void testGetURLByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.getURL(1);
    }

    @Test(expected = SQLException.class)
    public void testGetURLByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.getURL("column");
    }
    @Test(expected = SQLException.class)
    public void testGetURLByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.getURL("column");
    }

    @Test(expected = SQLException.class)
    public void testUpdateRefByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateRef(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateRefByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateRef(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateRefByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateRef("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateRefByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateRef("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateArrayByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateArray(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateArrayByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateArray(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateArrayByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateArray("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateArrayByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateArray("column", null);
    }

    @Test(expected = SQLException.class)
    public void testGetRowIdByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.getRowId(1);
    }

    @Test(expected = SQLException.class)
    public void testGetRowIdByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.getRowId(1);
    }

    @Test(expected = SQLException.class)
    public void testGetRowIdByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.getRowId("column");
    }
    @Test(expected = SQLException.class)
    public void testGetRowIdByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.getRowId("column");
    }

    @Test(expected = SQLException.class)
    public void testUpdateRowIdByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateRowId(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateRowIdByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateRowId(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateRowIdByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateRowId("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateRowIdByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateRowId("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNStringByIndex() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNString(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNStringByIndex2() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNString(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNStringByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNString("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNStringByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNString("column", null);
    }


    @Test(expected = SQLException.class)
    public void testUpdateNClobByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNClob(1, (NClob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNClob(1, (NClob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNClob("column", (NClob) null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNClobByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNClob("column", (NClob) null);
    }

    @Test(expected = SQLException.class)
    public void testGetNClobByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.getNClob(1);
    }
    @Test(expected = SQLException.class)
    public void testGetNClobByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.getNClob(1);
    }

    @Test(expected = SQLException.class)
    public void testGetNClobByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.getNClob("column");
    }
    @Test(expected = SQLException.class)
    public void testGetNClobByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.getNClob("column");
    }

    @Test(expected = SQLException.class)
    public void testGetSQLXMLByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.getSQLXML(1);
    }
    @Test(expected = SQLException.class)
    public void testGetSQLXMLByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.getSQLXML(1);
    }

    @Test(expected = SQLException.class)
    public void testGetSQLXMLByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.getSQLXML("column");
    }
    @Test(expected = SQLException.class)
    public void testGetSQLXMLByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.getSQLXML("column");
    }

    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateSQLXML(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateSQLXML(1, null);
    }


    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateSQLXML("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateSQLXML("column", null);
    }


    @Test(expected = SQLException.class)
    public void testGetNCharacterStreamByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.getNCharacterStream(1);
    }
    @Test(expected = SQLException.class)
    public void testGetNCharacterStreamByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.getNCharacterStream(1);
    }


    @Test(expected = SQLException.class)
    public void testGetNCharacterStreamByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.getNCharacterStream("column");
    }
    @Test(expected = SQLException.class)
    public void testGetNCharacterStreamByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.getNCharacterStream("column");
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNCharacterStream(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNCharacterStream(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNCharacterStream("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNCharacterStream("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateAsciiStream(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateAsciiStream(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBinaryStream(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBinaryStream(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateCharacterStream(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateCharacterStream(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateAsciiStream("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateAsciiStream("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBinaryStream("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBinaryStream("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateCharacterStream("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateCharacterStream("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBlob(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBlobByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBlob(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBlob("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBlobByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBlob("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateClob(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateClobByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateClob(1, null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateClob("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateClobByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateClob("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByIndexWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNClob(1, null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNClobByIndexWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNClob(1, null, 10L);
    }


    @Test(expected = SQLException.class)
    public void testUpdateNClobByLabelWithLength() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNClob("column", null, 10L);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNClobByLabelWithLength2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNClob("column", null, 10L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNCharacterStream(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNCharacterStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateNCharacterStream("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateNCharacterStream("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateAsciiStream(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateAsciiStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBinaryStream(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBinaryStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateCharacterStream(1, null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateCharacterStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateAsciiStream("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateAsciiStream("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBinaryStream("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBinaryStream("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateCharacterStream("column", null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateCharacterStream("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBlob(1, (Blob) null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBlobByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBlob(1, (Blob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateBlob("column", (Blob) null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateBlobByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateBlob("column", (Blob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByIndex() throws SQLException {
        resultSet.closed = false;
        resultSet.updateClob(1, (Clob) null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateClobByIndex2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateClob(1, (Clob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByLabel() throws SQLException {
        resultSet.closed = false;
        resultSet.updateClob("column", (Clob) null);
    }
    @Test(expected = SQLException.class)
    public void testUpdateClobByLabel2() throws SQLException {
        resultSet.closed = true;
        resultSet.updateClob("column", (Clob) null);
    }

    @Test(expected = SQLException.class)
    public void testGetObjectWithType() throws SQLException {
        resultSet.closed = false;
        resultSet.getObject(1, String.class);
    }
    @Test(expected = SQLException.class)
    public void testGetObjectWithType2() throws SQLException {
        resultSet.closed = true;
        resultSet.getObject(1, String.class);
    }


}