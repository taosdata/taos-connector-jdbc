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

import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class WebSocketDriverTest {
    static final String HOST = TestEnvUtil.getHost();

    @Test
    public void acceptsURL() throws SQLException {
        Driver driver = new WebSocketDriver();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort();
        }
        boolean isAccept = driver.acceptsURL(url);
        Assert.assertTrue(isAccept);
        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getWsPort();
        } else {
            url = "jdbc:TAOS://" + specifyHost + ":" + TestEnvUtil.getWsPort();
        }
        isAccept = driver.acceptsURL(url);
        Assert.assertFalse(isAccept);
    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Driver driver = new WebSocketDriver();
        final String url = "";
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, null);
        Assert.assertNotNull(propertyInfo);
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void getMajorVersion() {
        Assert.assertEquals(3, new RestfulDriver().getMajorVersion());
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void getMinorVersion() {
        Assert.assertEquals(0, new RestfulDriver().getMinorVersion());
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void jdbcCompliant() {
        Assert.assertFalse(new RestfulDriver().jdbcCompliant());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParentLogger() throws SQLFeatureNotSupportedException {
        new WebSocketDriver().getParentLogger();
    }
}

