package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Test;

import java.sql.SQLException;

public class SchemaConnectTest {
    private static final String host = "127.0.0.1";

    @Test
    public void testUrl() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/information_schema?user=root&password=taosdata";
        }
        SchemalessWriter writer = new SchemalessWriter(url, null, null, null);
    }

    @Test
    public void testAllParam() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter("127.0.0.1", "6041", "root", "taosdata", "information_schema", "ws");
    }
}
