package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbsWSPreparedStatementQueryPrecisionTest {
    @Test
    public void queryStatement_usesNsPrecisionWhenServerSupportsQueryNs() throws Exception {
        TSWSPreparedStatement stmt = newQueryStmt(TSDBConstants.MIN_QUERY_NS_VERSION);

        assertEquals(TimestampPrecision.NS, stmt.stmtInfo.getPrecision());
    }

    @Test
    public void queryStatement_keepsMsPrecisionWhenServerDoesNotSupportQueryNs() throws Exception {
        TSWSPreparedStatement stmt = newQueryStmt("3.4.1.10");

        assertEquals(TimestampPrecision.MS, stmt.stmtInfo.getPrecision());
    }

    @Test
    public void insertStatement_keepsPrecisionFromPrepareResponse() throws Exception {
        TSWSPreparedStatement stmt = newInsertStmt(TSDBConstants.MIN_QUERY_NS_VERSION, TimestampPrecision.US);

        assertEquals(TimestampPrecision.US, stmt.stmtInfo.getPrecision());
    }

    private static TSWSPreparedStatement newQueryStmt(String serverVersion) throws SQLException {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setStmtId(1L);
        prepareResp.setInsert(false);
        prepareResp.setFieldsCount(1);
        return newStmt(serverVersion, prepareResp, "select * from t where ts > ?");
    }

    private static TSWSPreparedStatement newInsertStmt(String serverVersion, int precision) throws SQLException {
        Field timestampField = new Field();
        timestampField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        timestampField.setFieldType((byte) TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP);
        timestampField.setPrecision((byte) precision);

        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setStmtId(1L);
        prepareResp.setInsert(true);
        prepareResp.setFields(Collections.singletonList(timestampField));
        return newStmt(serverVersion, prepareResp, "insert into t values (?)");
    }

    private static TSWSPreparedStatement newStmt(String serverVersion,
                                                Stmt2PrepareResp prepareResp,
                                                String sql) throws SQLException {
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);

        ConnectionParam param = mock(ConnectionParam.class);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(transport.getConnectionParam()).thenReturn(param);

        AbstractConnection connection = new WSConnection(
                "jdbc:TAOS-WS://localhost:6041/test",
                new Properties(),
                transport,
                param,
                serverVersion);

        return new TSWSPreparedStatement(
                transport,
                param,
                "test_db",
                connection,
                sql,
                1L,
                prepareResp);
    }
}
