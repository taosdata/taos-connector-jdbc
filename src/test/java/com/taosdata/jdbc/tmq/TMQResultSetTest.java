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

package com.taosdata.jdbc.tmq;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class TMQResultSetTest {
    private TMQResultSet resultSet;

    @Before
    public void setUp() {
        // 假设 TMQResultSet 有一个适当的构造函数
        resultSet = new TMQResultSet(null, 0, 0);
    }

    @Test(expected = SQLException.class)
    public void testIsBeforeFirst_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsBeforeFirst_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsAfterLast_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void testIsAfterLast_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void testIsFirst_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.isFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsFirst_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.isFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsLast_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.isLast();
    }

    @Test(expected = SQLException.class)
    public void testIsLast_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.isLast();
    }

    @Test(expected = SQLException.class)
    public void testBeforeFirst_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testBeforeFirst_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testAfterLast_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.afterLast();
    }

    @Test(expected = SQLException.class)
    public void testAfterLast_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.afterLast();
    }

    @Test(expected = SQLException.class)
    public void testFirst_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.first();
    }

    @Test(expected = SQLException.class)
    public void testFirst_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.first();
    }

    @Test(expected = SQLException.class)
    public void testLast_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.last();
    }

    @Test(expected = SQLException.class)
    public void testLast_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.last();
    }

    @Test(expected = SQLException.class)
    public void testGetRow_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.getRow();
    }

    @Test(expected = SQLException.class)
    public void testGetRow_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.getRow();
    }

    @Test(expected = SQLException.class)
    public void testAbsolute_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.absolute(1);
    }

    @Test(expected = SQLException.class)
    public void testAbsolute_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.absolute(1);
    }

    @Test(expected = SQLException.class)
    public void testRelative_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.relative(1);
    }

    @Test(expected = SQLException.class)
    public void testRelative_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.relative(1);
    }

    @Test(expected = SQLException.class)
    public void testPrevious_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.previous();
    }

    @Test(expected = SQLException.class)
    public void testPrevious_UnsupportedMethod() throws SQLException {
        // 这里可以添加逻辑以确保抛出异常
        resultSet.previous();
    }

    @Test(expected = SQLException.class)
    public void testGetStatement_ConnectionClosed() throws SQLException {
        resultSet.close(); // 模拟连接关闭
        resultSet.getStatement();
    }

    @Test
    public void testGetStatement() throws SQLException {
        // 这里可以添加逻辑以确保返回 null 或其他预期值
        assertNull(resultSet.getStatement());
    }
}
