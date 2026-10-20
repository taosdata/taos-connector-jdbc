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
        AbsWSPreparedStatement stmt = newQueryStmt(TSDBConstants.MIN_QUERY_NS_VERSION);

        assertEquals(TimestampPrecision.NS, stmt.stmtInfo.getPrecision());
    }

    @Test
    public void queryStatement_keepsMsPrecisionWhenServerDoesNotSupportQueryNs() throws Exception {
        AbsWSPreparedStatement stmt = newQueryStmt("3.4.1.10");

        assertEquals(TimestampPrecision.MS, stmt.stmtInfo.getPrecision());
    }

    @Test
    public void insertStatement_keepsPrecisionFromPrepareResponse() throws Exception {
        AbsWSPreparedStatement stmt = newInsertStmt(TSDBConstants.MIN_QUERY_NS_VERSION, TimestampPrecision.US);

        assertEquals(TimestampPrecision.US, stmt.stmtInfo.getPrecision());
    }

    private static AbsWSPreparedStatement newQueryStmt(String serverVersion) throws SQLException {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setStmtId(1L);
        prepareResp.setInsert(false);
        prepareResp.setFieldsCount(1);
        return newStmt(serverVersion, prepareResp, "select * from t where ts > ?");
    }

    private static AbsWSPreparedStatement newInsertStmt(String serverVersion, int precision) throws SQLException {
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

    private static AbsWSPreparedStatement newStmt(String serverVersion,
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

        return new AbsWSPreparedStatement(
                transport,
                param,
                "test_db",
                connection,
                sql,
                1L,
                prepareResp);
    }
}
