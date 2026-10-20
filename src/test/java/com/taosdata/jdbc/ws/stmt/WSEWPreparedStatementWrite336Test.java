/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSEWPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class WSEWPreparedStatementWrite336Test extends AbstractWSEWPreparedStatementWriteTest {
    public WSEWPreparedStatementWrite336Test() {
        super("traditional", WSEWPreparedStatement.class);
    }

    @Override
    protected String stableInsertSql() {
        return WsStmtWriteTestSupport.stableInsertSql336(dbName, stableName);
    }

    @Override
    protected String asyncInsertSql() {
        return WsStmtWriteTestSupport.asyncInsertSql336(dbName, stableName);
    }

    @Override
    protected void createStable(java.sql.Statement statement) throws java.sql.SQLException {
        WsStmtWriteTestSupport.createAllTypeStable336(statement, dbName, stableName);
    }

    @Override
    protected void bindAllTypes(java.sql.PreparedStatement statement, int startIndex, long current)
            throws java.sql.SQLException {
        WsStmtWriteTestSupport.bindAllTypes336(statement, startIndex, current);
    }

    @Override
    protected void bindNullTypes(java.sql.PreparedStatement statement, int startIndex, long current)
            throws java.sql.SQLException {
        WsStmtWriteTestSupport.bindNullTypes336(statement, startIndex, current);
    }

    @Override
    protected void assertAllTypeRow(java.sql.ResultSet resultSet, long current) throws java.sql.SQLException {
        WsStmtWriteTestSupport.assertAllTypeRow336(resultSet, current);
    }

    @Override
    protected void assertNullTypeRow(java.sql.ResultSet resultSet, long current) throws java.sql.SQLException {
        WsStmtWriteTestSupport.assertNullTypeRow336(resultSet, current);
    }

    @Override
    @Ignore("testPrecreatedSubtable_roundTripsNulls runs only in the main-version WSEW write tests")
    @Test
    public void testPrecreatedSubtable_roundTripsNulls() {
    }

    @BeforeClass
    public static void setUp336() {
        TestUtils.runIn336();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown336() {
        System.gc();
    }
}
