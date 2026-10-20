/*
 * Copyright (c) 2024 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.tmq.ReferenceDeserializer;
import com.taosdata.jdbc.ws.entity.QueryResp;
import com.taosdata.jdbc.ws.tmq.WSConsumerResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.TimeZone;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
public class WSConsumerResultSetTest {

    private WSConsumerResultSet wsConsumerResultSet;
    private TimeZone originalTimeZone;

    @Before
    public void setUp() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        wsConsumerResultSet = new WSConsumerResultSet(null, null, 0, null, null);
    }

    @After
    public void tearDown() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    public void testTimestampStringUsesConsumerTimeZone() throws SQLException {
        long[] values = {1704067200123L, 1704067200123456L, 1704067200123456789L};
        int[] precisions = {TimestampPrecision.MS, TimestampPrecision.US, TimestampPrecision.NS};
        String[] fractions = {"123", "123456", "123456789"};

        for (int index = 0; index < values.length; index++) {
            WSConsumerResultSet resultSet = new TimestampResultSet(values[index], precisions[index], ZoneId.of("Asia/Tokyo"));
            String expected = "2024-01-01 09:00:00." + fractions[index];
            Instant expectedInstant = Instant.parse("2024-01-01T00:00:00." + fractions[index] + "Z");

            assertEquals(expected, resultSet.getString(1));
            assertFalse(resultSet.wasNull());
            assertEquals(expected, resultSet.getString("ts"));
            assertEquals(expected, resultSet.getNString(1));
            assertEquals(expectedInstant, resultSet.getTimestamp(1).toInstant());
            assertEquals(expectedInstant, ((Timestamp) resultSet.getObject(1)).toInstant());
        }
    }

    @Test
    public void testTimestampStringUsesJvmTimeZoneWhenConsumerTimeZoneIsNull() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        WSConsumerResultSet resultSet = new TimestampResultSet(1704067200000L, TimestampPrecision.MS, null);

        assertEquals("2023-12-31 19:00:00.0", resultSet.getString(1));
        assertEquals(1704067200000L, resultSet.getTimestamp(1).getTime());
    }

    @Test
    public void testTimestampStringPreservesNull() throws SQLException {
        WSConsumerResultSet resultSet = new TimestampResultSet(null, TimestampPrecision.MS, ZoneId.of("Asia/Tokyo"));

        assertNull(resultSet.getString(1));
        assertTrue(resultSet.wasNull());
    }

    @Test
    public void testReferenceDeserializerTimestampStringUsesConsumerTimeZone() throws Exception {
        WSConsumerResultSet resultSet = new TimestampResultSet(1704067200000L, TimestampPrecision.MS, ZoneId.of("Asia/Tokyo"));
        ReferenceDeserializer<TimestampStringBean> deserializer = new ReferenceDeserializer<TimestampStringBean>() {};

        TimestampStringBean bean = deserializer.deserialize(resultSet, "topic", "database");

        assertEquals("2024-01-01 09:00:00.0", bean.getTs());
    }

    @Test
    public void testQueryTimestampStringsUseConnectionTimeZone() throws SQLException {
        Instant instant = Instant.parse("2024-01-01T00:00:00.123456789Z");
        try (BlockResultSet resultSet = new TimestampQueryResultSet(instant, ZoneId.of("Asia/Tokyo"))) {
            String expected = "2024-01-01 09:00:00.123456789";

            assertEquals(expected, resultSet.getString(1));
            assertEquals(expected, resultSet.getString("ts"));
            assertEquals(expected, resultSet.getNString(1));
            assertEquals(expected, resultSet.getNString("ts"));
            assertEquals(expected, resultSet.getObject(1, String.class));
            assertEquals(expected, resultSet.getObject("ts", String.class));
            assertEquals(instant, resultSet.getTimestamp(1).toInstant());
            assertEquals(instant, ((Timestamp) resultSet.getObject(1)).toInstant());
        }
    }

    @Test
    public void testQueryTimestampStringsUseJvmTimeZoneWhenConnectionTimeZoneIsNull() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        Instant instant = Instant.parse("2024-01-01T00:00:00Z");
        try (BlockResultSet resultSet = new TimestampQueryResultSet(instant, null)) {
            assertEquals("2023-12-31 19:00:00.0", resultSet.getString(1));
            assertEquals(resultSet.getString(1), resultSet.getObject(1, String.class));
            assertEquals(instant, resultSet.getTimestamp(1).toInstant());
        }
    }

    @Test
    public void testQueryTimestampStringsPreserveNull() throws SQLException {
        try (BlockResultSet resultSet = new TimestampQueryResultSet(null, ZoneId.of("Asia/Tokyo"))) {
            assertNull(resultSet.getString(1));
            assertTrue(resultSet.wasNull());
            assertNull(resultSet.getObject(1, String.class));
            assertTrue(resultSet.wasNull());
        }
    }

    @Test
    public void testTextTimestampUsesConsumerTimeZoneLikeQuery() throws SQLException {
        String[] fractions = {"123", "123456", "123456789"};
        for (String fraction : fractions) {
            assertTextTimestampConversion("2024-01-01 00:00:00." + fraction, ZoneId.of("Asia/Tokyo"),
                    Instant.parse("2023-12-31T15:00:00." + fraction + "Z"));
        }
    }

    @Test
    public void testTextTimestampUsesJvmTimeZoneWhenConsumerTimeZoneIsNull() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));

        assertTextTimestampConversion("2024-01-01 00:00:00.123456789", null,
                Instant.parse("2024-01-01T05:00:00.123456789Z"));
    }

    @Test
    public void testTextTimestampPreservesNullAndInvalidValues() throws SQLException {
        assertTextTimestampConversion(null, ZoneId.of("Asia/Tokyo"), null);
        assertTextTimestampConversion("invalid timestamp", ZoneId.of("Asia/Tokyo"), null);
    }

    private void assertTextTimestampConversion(String literal, ZoneId zoneId, Instant expected) throws SQLException {
        int[] dataTypes = {TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_NCHAR};
        boolean[] varcharAsStrings = {true, false, false};
        for (int index = 0; index < dataTypes.length; index++) {
            Object source = literal == null ? null : dataTypes[index] == TSDB_DATA_TYPE_NCHAR
                    ? literal.codePoints().toArray() : literal.getBytes(StandardCharsets.UTF_8);
            try (WSConsumerResultSet consumer = new TimestampResultSet(source, dataTypes[index], zoneId, varcharAsStrings[index]);
                 BlockResultSet query = new TimestampQueryResultSet(source, dataTypes[index], zoneId, varcharAsStrings[index])) {
                Timestamp expectedTimestamp = expected == null ? null : Timestamp.from(expected);
                assertEquals(expectedTimestamp, query.getTimestamp(1));
                assertEquals(expectedTimestamp, consumer.getTimestamp(1));
                assertEquals(expected == null, consumer.wasNull());
                assertEquals(query.getTimestamp("ts"), consumer.getTimestamp("ts"));
                assertEquals(expected == null, consumer.wasNull());
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testIsBeforeFirst() throws SQLException {
        wsConsumerResultSet.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsAfterLast() throws SQLException {
        wsConsumerResultSet.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void testIsFirst() throws SQLException {
        wsConsumerResultSet.isFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsLast() throws SQLException {
        wsConsumerResultSet.isLast();
    }

    @Test(expected = SQLException.class)
    public void testBeforeFirst() throws SQLException {
        wsConsumerResultSet.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testafterFirst() throws SQLException {
        wsConsumerResultSet.afterLast();
    }

    @Test(expected = SQLException.class)
    public void testFirst() throws SQLException {
        wsConsumerResultSet.first();
    }

    @Test(expected = SQLException.class)
    public void testLast() throws SQLException {
        wsConsumerResultSet.last();
    }

    @Test(expected = SQLException.class)
    public void testGetRow() throws SQLException {
        assertEquals(0, wsConsumerResultSet.getRow());
    }

    @Test(expected = SQLException.class)
    public void testAbsolute() throws SQLException {
        wsConsumerResultSet.absolute(0);
    }

    @Test(expected = SQLException.class)
    public void testRelative() throws SQLException {
        wsConsumerResultSet.relative(0);
    }

    @Test(expected = SQLException.class)
    public void testPrevious() throws SQLException {
        wsConsumerResultSet.previous();
    }
    @Test(expected = SQLException.class)
    public void testGetStatement() throws SQLException {
        wsConsumerResultSet.getStatement();
    }

    private static class TimestampResultSet extends WSConsumerResultSet {
        TimestampResultSet(Long value, int precision, ZoneId zoneId) {
            super(null, null, 0, null, zoneId);
            fields = Collections.singletonList(new RestfulResultSet.Field("ts", Types.TIMESTAMP, 8, "", TSDB_DATA_TYPE_TIMESTAMP, 0, 0));
            columnNames = Collections.singletonList("ts");
            result = Collections.singletonList(Collections.<Object>singletonList(value));
            numOfRows = 1;
            timestampPrecision = precision;
        }

        TimestampResultSet(Object source, int dataType, ZoneId zoneId, boolean varcharAsString) {
            super(TimestampQueryResultSet.createTransport(varcharAsString), null, 0, null, zoneId);
            fields = Collections.singletonList(new RestfulResultSet.Field("ts", Types.VARCHAR, 64, "", dataType, 0, 0));
            columnNames = Collections.singletonList("ts");
            result = Collections.singletonList(Collections.<Object>singletonList(source));
            numOfRows = 1;
        }
    }

    private static class TimestampQueryResultSet extends BlockResultSet {
        TimestampQueryResultSet(Instant instant, ZoneId zoneId) throws SQLException {
            this(instant, TSDB_DATA_TYPE_TIMESTAMP, zoneId, false);
        }

        TimestampQueryResultSet(Object source, int dataType, ZoneId zoneId, boolean varcharAsString) throws SQLException {
            super(null, createTransport(varcharAsString), createResponse(dataType), null, zoneId);
            result = Collections.singletonList(Collections.<Object>singletonList(source));
            numOfRows = 1;
        }

        private static Transport createTransport(boolean varcharAsString) {
            Transport transport = mock(Transport.class);
            ConnectionParam param = mock(ConnectionParam.class);
            when(param.isVarcharAsString()).thenReturn(varcharAsString);
            when(transport.getConnectionParam()).thenReturn(param);
            return transport;
        }

        private static QueryResp createResponse(int dataType) {
            QueryResp response = new QueryResp();
            response.setFieldsCount(1);
            response.setFieldsNames(new String[]{"ts"});
            response.setFieldsTypes(new int[]{dataType});
            response.setFieldsLengths(new int[]{8});
            response.setPrecision(TimestampPrecision.NS);
            return response;
        }
    }

    public static class TimestampStringBean {
        private String ts;

        public String getTs() {
            return ts;
        }

        public void setTs(String ts) {
            this.ts = ts;
        }
    }

}
