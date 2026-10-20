/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that setTimestamp binding is independent of the connection zone:
 * a setTimezone() call between two binds on the same statement does not change
 * the bound instant. Transport is stubbed with Mockito; no live server is required.
 */
public class AbsWSPreparedStatementBindTimezoneTest {

    @Test
    public void setTimestampWithCalendarPreservesInstant() throws Exception {
        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            Transport transport = mock(Transport.class);
            when(transport.getReconnectCount()).thenReturn(0);
            ConnectionParam param = mock(ConnectionParam.class);
            when(param.getZoneId()).thenReturn(ZoneId.of("Asia/Shanghai"));
            when(param.getRequestTimeout()).thenReturn(5000);
            when(transport.getConnectionParam()).thenReturn(param);

            Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
            prepareResp.setStmtId(1L);
            try (AbsWSPreparedStatement stmt = new AbsWSPreparedStatement(
                    transport, param, "test_db", mock(WSConnection.class),
                    "INSERT INTO t VALUES (?)", 1L, prepareResp)) {
                Timestamp timestamp = Timestamp.valueOf("2024-01-01 09:00:00.123456789");
                Instant expected = Instant.parse("2024-01-01T09:00:00.123456789Z");
                Calendar[] calendars = {
                        Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo")),
                        Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles")),
                        null
                };

                stmt.setTimestamp(1, timestamp);
                assertEquals(expected, stmt.colOrderedMap.get(1).getData());
                for (Calendar calendar : calendars) {
                    stmt.setTimestamp(1, timestamp, calendar);
                    assertEquals("Calendar must preserve the bound instant: "
                                    + (calendar == null ? "null" : calendar.getTimeZone().getID()),
                            expected, stmt.colOrderedMap.get(1).getData());
                }
            }
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test
    public void setTimestampIsZoneIndependent() throws Exception {
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);

        ConnectionParam param = mock(ConnectionParam.class);
        // zone id changes between the two binds, as if setTimezone() was called in between
        when(param.getZoneId()).thenReturn(ZoneId.of("Asia/Tokyo"), ZoneId.of("Asia/Shanghai"));
        when(param.getRequestTimeout()).thenReturn(5000);
        when(transport.getConnectionParam()).thenReturn(param);

        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setStmtId(1L);
        AbsWSPreparedStatement stmt = new AbsWSPreparedStatement(
                transport,
                param,
                "test_db",
                mock(AbstractConnection.class),
                "INSERT INTO t VALUES (?)",
                1L,
                prepareResp);

        Timestamp ts = Timestamp.valueOf("2024-01-02 03:04:05");

        stmt.setTimestamp(1, ts);
        Column tokyoColumn = stmt.colOrderedMap.get(1);
        Instant tokyoInstant = (Instant) tokyoColumn.getData();

        stmt.setTimestamp(1, ts);
        Column shanghaiColumn = stmt.colOrderedMap.get(1);
        Instant shanghaiInstant = (Instant) shanghaiColumn.getData();

        // the connection zone must not shift the bound value: both binds carry
        // exactly the instant of the given Timestamp
        assertEquals(ts.toInstant(), tokyoInstant);
        assertEquals(ts.toInstant(), shanghaiInstant);
    }
}
