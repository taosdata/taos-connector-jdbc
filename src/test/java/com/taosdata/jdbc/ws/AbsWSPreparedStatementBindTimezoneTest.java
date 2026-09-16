package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that PreparedStatement bind resolves the zone id at bind time, so a
 * setTimezone() call between two binds on the same statement takes effect.
 * Transport is stubbed with Mockito; no live server is required.
 */
public class AbsWSPreparedStatementBindTimezoneTest {

    @Test
    public void setTimestampUsesZoneIdAtBindTime() throws Exception {
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

        // the same wall clock binds one hour earlier in Tokyo (+09:00) than in Shanghai (+08:00)
        assertEquals(3600_000L, shanghaiInstant.toEpochMilli() - tokyoInstant.toEpochMilli());
    }
}
