package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AbstractDatabaseMetaDataTest {
    Connection connection;
    String host = "127.0.0.1";

    @Test
    public void test() throws IOException, SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataTest.class.getClassLoader().getResourceAsStream("version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
    }

    @Before
    public void before() throws Exception {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        connection = DriverManager.getConnection(url);
    }

}