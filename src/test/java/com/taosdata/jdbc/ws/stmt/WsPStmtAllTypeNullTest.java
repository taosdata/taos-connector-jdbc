package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.sql.*;
import java.util.Collections;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WsPStmtAllTypeNullTest {
    String host = "127.0.0.1";
    String db_name = TestUtils.camelToSnake(WsPStmtAllTypeNullTest.class);
    String tableName = "wpt";
    String stableName = "swpt";
    Connection connection;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        long current = System.currentTimeMillis();
        statement.setTimestamp(1, new Timestamp(current));
        statement.setNull(2, Types.TINYINT);
        statement.setNull(3, Types.SMALLINT);
        statement.setNull(4, Types.INTEGER);
        statement.setNull(5, Types.BIGINT);
        statement.setNull(6, Types.FLOAT);
        statement.setNull(7, Types.DOUBLE);
        statement.setNull(8, Types.BOOLEAN);

        statement.setString(9, null);
        statement.setNString(10, null);
        statement.setString(11, null);
        statement.setNull(12, Types.VARBINARY);
        statement.setNull(13, Types.VARBINARY);
        statement.executeUpdate();

        ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName);
        resultSet.next();
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));

        resultSet.getByte(2);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getShort(3);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getInt(4);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getLong(5);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getFloat(6);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getDouble(7);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getBoolean(8);
        Assert.assertTrue(resultSet.wasNull());

        Assert.assertNull(resultSet.getString(9));
        Assert.assertNull(resultSet.getString(10));
        Assert.assertNull(resultSet.getString(11));

        Assert.assertNull(resultSet.getBytes(12));
        Assert.assertNull(resultSet.getBytes(13));
        resultSet.close();
        statement.close();
    }


    @Test
    public void testExecuteUpdate2() throws SQLException {
        String sql = "insert into stb_1 using " + db_name + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?)";
        TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class);
        long current = System.currentTimeMillis();

        statement.setTagNull(0, TSDB_DATA_TYPE_TIMESTAMP);
        statement.setTagNull(1, TSDB_DATA_TYPE_TINYINT);
        statement.setTagNull(2, TSDB_DATA_TYPE_SMALLINT);
        statement.setTagNull(3, TSDB_DATA_TYPE_INT);
        statement.setTagNull(4, TSDB_DATA_TYPE_BIGINT);
        statement.setTagNull(5, TSDB_DATA_TYPE_FLOAT);
        statement.setTagNull(6, TSDB_DATA_TYPE_DOUBLE);
        statement.setTagNull(7, TSDB_DATA_TYPE_BOOL);

        statement.setTagNull(8, TSDB_DATA_TYPE_BINARY);
        statement.setTagNull(9, TSDB_DATA_TYPE_NCHAR);
        statement.setTagNull(10, TSDB_DATA_TYPE_BINARY);

        statement.setTagNull(11, TSDB_DATA_TYPE_VARBINARY);
        statement.setTagNull(12, TSDB_DATA_TYPE_GEOMETRY);


        statement.setTagNull(13, TSDB_DATA_TYPE_UTINYINT);
        statement.setTagNull(14, TSDB_DATA_TYPE_USMALLINT);
        statement.setTagNull(15, TSDB_DATA_TYPE_UINT);
        statement.setTagNull(16, TSDB_DATA_TYPE_UBIGINT);


        statement.setTimestamp(0, Collections.singletonList(current));
        statement.setByte(1, Collections.singletonList(null));
        statement.columnDataAddBatch();
        statement.columnDataExecuteBatch();

        ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + stableName);
        resultSet.next();


        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));


        resultSet.getByte(2);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getTimestamp(3);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getByte(4);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getShort(5);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getInt(6);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getLong(7);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getFloat(8);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getDouble(9);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getBoolean(10);
        Assert.assertTrue(resultSet.wasNull());

        Assert.assertNull(resultSet.getString(11));
        Assert.assertNull(resultSet.getString(12));
        Assert.assertNull(resultSet.getString(13));

        Assert.assertNull(resultSet.getBytes(14));
        Assert.assertNull(resultSet.getBytes(15));

        resultSet.getShort(16);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getInt(17);
        Assert.assertTrue(resultSet.wasNull());

        resultSet.getLong(18);
        Assert.assertTrue(resultSet.wasNull());

        Object obj = resultSet.getObject(19);
        Assert.assertNull(obj);


        resultSet.close();
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name + " keep 36500");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100))");

        statement.execute("create stable if not exists " + db_name + "." + stableName +
                "(ts timestamp, c1 tinyint) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, " +
                "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), t12 varbinary(100), t13 geometry(100)," +
                " t14 tinyint unsigned, t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");

        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()){
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
