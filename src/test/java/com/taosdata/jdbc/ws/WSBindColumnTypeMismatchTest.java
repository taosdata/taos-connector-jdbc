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

import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Regression tests for the legacy (traditional) row-bind path: when the bind type
 * differed from the column type, the driver swapped the column type metadata without
 * converting the value, which threw ClassCastException in SerializeBlock (e.g. setInt
 * into a TINYINT column, which is what Apache Spark does for ByteType/ShortType).
 * The connection always uses a WebSocket URL with stmtBindMode=traditional so the
 * legacy path is exercised on any server version; SpecifyAddress is deliberately not
 * honored here because it may point at the REST implementation, which never touches
 * SerializeBlock.
 */
public class WSBindColumnTypeMismatchTest {

    private static final String DB = "ws_bind_type_mismatch_test";
    private static Connection conn;

    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort()
                + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword()
                + "&stmtBindMode=traditional";
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB);
            stmt.execute("create database " + DB);
            stmt.execute("use " + DB);
        }
    }

    @AfterClass
    public static void afterClass() {
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists " + DB);
                conn.close();
            } catch (SQLException ignored) {
                // nothing
            }
        }
    }

    private static void assertOutOfRange(String columnType, Binder binder) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists t_range");
            stmt.execute("create table t_range (ts timestamp, v " + columnType + ")");
        }
        try (PreparedStatement ps = conn.prepareStatement("insert into t_range values(?, ?)")) {
            ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            binder.bind(ps);
            ps.addBatch();
            ps.executeBatch();
            Assert.fail("expected an out-of-range SQLException for " + columnType);
        } catch (SQLException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void setIntIntoTinyintAndSmallint() throws SQLException {
        // what Apache Spark does for ByteType/ShortType columns: bind via setInt
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists t_ts");
            stmt.execute("create table t_ts (ts timestamp, v_t tinyint, v_s smallint)");
        }
        long ts = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("insert into t_ts values(?, ?, ?)")) {
            ps.setTimestamp(1, new Timestamp(ts));
            ps.setInt(2, 127);
            ps.setInt(3, 32767);
            ps.addBatch();
            ps.setTimestamp(1, new Timestamp(ts + 1));
            ps.setNull(2, Types.TINYINT);
            ps.setNull(3, Types.SMALLINT);
            ps.addBatch();
            ps.executeBatch();
        }
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select v_t, v_s from t_ts order by ts")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(127, rs.getInt(1));
            Assert.assertEquals(32767, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getObject(1));
            Assert.assertNull(rs.getObject(2));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void mismatchedNumericBinds() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists t_num");
            stmt.execute("create table t_num (ts timestamp, v_i int, v_b bigint, v_f float, v_d double)");
        }
        long ts = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("insert into t_num values(?, ?, ?, ?, ?)")) {
            ps.setTimestamp(1, new Timestamp(ts));
            ps.setLong(2, 123456L);      // bigint bind into int column
            ps.setInt(3, 456);           // int bind into bigint column
            ps.setDouble(4, 1.5d);       // double bind into float column
            ps.setFloat(5, 2.5f);        // float bind into double column
            ps.addBatch();
            ps.executeBatch();
        }
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select v_i, v_b, v_f, v_d from t_num")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(123456, rs.getInt(1));
            Assert.assertEquals(456L, rs.getLong(2));
            Assert.assertEquals(1.5f, rs.getFloat(3), 0.0f);
            Assert.assertEquals(2.5d, rs.getDouble(4), 0.0d);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void mismatchedUnsignedBinds() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists t_unsigned");
            stmt.execute("create table t_unsigned (ts timestamp, a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned)");
        }
        long ts = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("insert into t_unsigned values(?, ?, ?, ?, ?)")) {
            ps.setTimestamp(1, new Timestamp(ts));
            ps.setInt(2, 200);
            ps.setInt(3, 60000);
            ps.setLong(4, 3000000000L);
            ps.setLong(5, 15000000000L);
            ps.addBatch();
            // double bind into the ubigint column goes through a BigDecimal
            // intermediate instead of a saturating longValue()
            ps.setTimestamp(1, new Timestamp(ts + 1));
            ps.setDouble(2, 42.0d);
            ps.setDouble(3, 43.0d);
            ps.setDouble(4, 44.0d);
            ps.setDouble(5, 45.0d);
            ps.addBatch();
            ps.executeBatch();
        }
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select a, b, c, d from t_unsigned order by ts")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(200, rs.getInt(1));
            Assert.assertEquals(60000, rs.getInt(2));
            Assert.assertEquals(3000000000L, rs.getLong(3));
            Assert.assertEquals(new BigInteger("15000000000"), rs.getObject(4));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(42, rs.getInt(1));
            Assert.assertEquals(43, rs.getInt(2));
            Assert.assertEquals(44L, rs.getLong(3));
            Assert.assertEquals(new BigInteger("45"), rs.getObject(4));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void booleanNumericCrossBinds() throws SQLException {
        // same acceptance matrix as the stmt2 column-bind path: Boolean binds as 1/0
        // for numeric columns, a Number binds into BOOL as intValue() != 0
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists t_cross");
            stmt.execute("create table t_cross (ts timestamp, v_b bool, v_t tinyint, v_d double)");
        }
        long ts = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("insert into t_cross values(?, ?, ?, ?)")) {
            ps.setTimestamp(1, new Timestamp(ts));
            ps.setInt(2, 5);
            ps.setBoolean(3, true);
            ps.setBoolean(4, true);
            ps.addBatch();
            ps.setTimestamp(1, new Timestamp(ts + 1));
            ps.setInt(2, 0);
            ps.setBoolean(3, false);
            ps.setBoolean(4, false);
            ps.addBatch();
            ps.executeBatch();
        }
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select v_b, v_t, v_d from t_cross order by ts")) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertEquals(1.0d, rs.getDouble(3), 0.0d);
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertEquals(0, rs.getInt(2));
            Assert.assertEquals(0.0d, rs.getDouble(3), 0.0d);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void signedNarrowingOutOfRangeFails() throws SQLException {
        assertOutOfRange("tinyint", ps -> ps.setInt(2, 200));
        assertOutOfRange("tinyint", ps -> ps.setInt(2, -200));
        assertOutOfRange("smallint", ps -> ps.setInt(2, 70000));
        assertOutOfRange("int", ps -> ps.setLong(2, 5000000000L));
        assertOutOfRange("float", ps -> ps.setDouble(2, 1e300d));
        // Long.MAX_VALUE.intValue() wraps to -1, which would bypass an intValue()-based
        // range check; BigInteger beyond the long range would wrap on longValue();
        // BigDecimal("1e310").doubleValue() saturates to Infinity; NaN/Infinity cannot
        // be held by an integer column
        assertOutOfRange("tinyint", ps -> ps.setLong(2, Long.MAX_VALUE));
        assertOutOfRange("int", ps -> ps.setObject(2, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
        assertOutOfRange("float", ps -> ps.setObject(2, new BigDecimal("1e310")));
        assertOutOfRange("tinyint", ps -> ps.setDouble(2, Double.NaN));
        assertOutOfRange("int", ps -> ps.setDouble(2, Double.POSITIVE_INFINITY));
        // BigInteger past the long range wraps on longValue(); a double beyond the
        // long range saturates to Long.MAX_VALUE; both must be rejected for BIGINT
        assertOutOfRange("bigint", ps -> ps.setObject(2, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
        assertOutOfRange("bigint", ps -> ps.setDouble(2, 1e30d));
        assertOutOfRange("bigint", ps -> ps.setDouble(2, Double.NaN));
    }

    @Test
    public void unsignedOutOfRangeStillFails() throws SQLException {
        assertOutOfRange("tinyint unsigned", ps -> ps.setInt(2, 300));
        assertOutOfRange("smallint unsigned", ps -> ps.setInt(2, -1));
        assertOutOfRange("int unsigned", ps -> ps.setLong(2, -1L));
        // 1e20 exceeds the ubigint range and must not saturate through longValue();
        // a negative double must not wrap into the unsigned range either
        assertOutOfRange("bigint unsigned", ps -> ps.setDouble(2, 1e20d));
        assertOutOfRange("bigint unsigned", ps -> ps.setDouble(2, -1.5d));
    }
}
