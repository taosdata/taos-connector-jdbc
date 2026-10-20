/*
 * Copyright (c) 2023 TAOS Data, Inc.
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

package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AbstractDatabaseMetaDataVesionTest {
    Connection connection;
    static final String host = TestEnvUtil.getHost();

    @Test
    public void testJni() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

    @Test
    public void testRest() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

    @Test
    public void testWebsocket() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties config = new Properties();
        config.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, config);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

}