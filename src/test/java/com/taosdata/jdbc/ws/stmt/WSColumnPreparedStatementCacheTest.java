package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Verify PreparedStatement caching for {@link WSColumnPreparedStatement},
 * the stmt2 bind-exec insert path.
 */
public class WSColumnPreparedStatementCacheTest {

    private final String dbName = WsStmtWriteTestSupport.dbName(
            WSColumnPreparedStatementCacheTest.class);
    private final String tableName = "col_cache_test";
    private Connection connection;

    @BeforeClass
    public static void setUp() {
        TestUtils.runInMain();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
    }

    @Before
    public void before() throws SQLException {
        connection = WsStmtWriteTestSupport.openWebSocketConnection("column", false);
        WsStmtWriteTestSupport.assumeBindExecSupported(connection,
                "WSColumnPreparedStatement cache");
        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + dbName + "." + tableName + " (ts TIMESTAMP, v INT)");
        }
    }

    @After
    public void after() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            WsStmtWriteTestSupport.dropDatabase(connection, dbName);
            connection.close();
        }
    }

    @Test
    public void testBasicCache() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement ps1 = connection.prepareStatement(sql);
        Assert.assertTrue("Should create WSColumnPreparedStatement",
                ps1 instanceof WSColumnPreparedStatement);
        long id1 = ((WSColumnPreparedStatement) ps1).getInstanceId();
        ps1.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        ps1.setInt(2, 1);
        ps1.executeUpdate();
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(sql);
        Assert.assertTrue(ps2 instanceof WSColumnPreparedStatement);
        long id2 = ((WSColumnPreparedStatement) ps2).getInstanceId();
        Assert.assertEquals("WSColumnPreparedStatement should be cached", id1, id2);
        ps2.close();
    }

    @Test
    public void testCacheDisabled() throws SQLException {
        connection.close();

        // Open with cacheSize=0, column mode
        String url = WsStmtWriteTestSupport.webSocketUrl(null, false, false);
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_STMT_BIND_MODE, "column");
        props.setProperty(TSDBDriver.PROPERTY_KEY_STMT_CACHE_SIZE, "0");
        connection = DriverManager.getConnection(url, props);

        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + dbName + "." + tableName + " (ts TIMESTAMP, v INT)");
        }

        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";
        PreparedStatement ps1 = connection.prepareStatement(sql);
        Assert.assertTrue(ps1 instanceof WSColumnPreparedStatement);
        long id1 = ((WSColumnPreparedStatement) ps1).getInstanceId();
        ps1.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        ps1.setInt(2, 1);
        ps1.executeUpdate();
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(sql);
        long id2 = ((WSColumnPreparedStatement) ps2).getInstanceId();
        ps2.close();
        Assert.assertNotEquals("stmtCacheSize=0 should not reuse", id1, id2);
    }

    @Test
    public void testMultipleReuse() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement first = connection.prepareStatement(sql);
        Assert.assertTrue(first instanceof WSColumnPreparedStatement);
        long firstId = ((WSColumnPreparedStatement) first).getInstanceId();
        first.close();

        for (int i = 0; i < 10; i++) {
            PreparedStatement ps = connection.prepareStatement(sql);
            Assert.assertTrue(ps instanceof WSColumnPreparedStatement);
            long id = ((WSColumnPreparedStatement) ps).getInstanceId();
            Assert.assertEquals("Iteration " + i + " should reuse cached WSColumnPreparedStatement",
                    firstId, id);
            ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
            ps.setInt(2, i);
            ps.executeUpdate();
            ps.close();
        }
    }

    @Test
    public void testConcurrentUse() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement ps1 = connection.prepareStatement(sql);
        Assert.assertTrue(ps1 instanceof WSColumnPreparedStatement);
        Assert.assertTrue(((WSColumnPreparedStatement) ps1).isInUse());

        PreparedStatement ps2 = connection.prepareStatement(sql);
        Assert.assertTrue(ps2 instanceof WSColumnPreparedStatement);
        Assert.assertNotEquals("Concurrent use should create new instance",
                ((WSColumnPreparedStatement) ps1).getInstanceId(),
                ((WSColumnPreparedStatement) ps2).getInstanceId());

        ps1.close();
        ps2.close();
    }

    @Test
    public void testOnlyCacheInsert() throws SQLException {
        String insertSql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";
        PreparedStatement ps1 = connection.prepareStatement(insertSql);
        Assert.assertTrue(ps1 instanceof WSColumnPreparedStatement);
        long id1 = ((WSColumnPreparedStatement) ps1).getInstanceId();
        ps1.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        ps1.setInt(2, 1);
        ps1.executeUpdate();
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(insertSql);
        long id2 = ((WSColumnPreparedStatement) ps2).getInstanceId();
        Assert.assertEquals("Insert should be cached", id1, id2);
        ps2.close();
    }

    @Test
    public void testConnectionCloseReleasesCache() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        for (int i = 0; i < 3; i++) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
            ps.setInt(2, i);
            ps.executeUpdate();
            ps.close();
        }

        connection.close();

        // Reconnect — should work fine because cached stmts were released
        connection = WsStmtWriteTestSupport.openWebSocketConnection("column", false);

        PreparedStatement ps = connection.prepareStatement(sql);
        Assert.assertTrue(ps instanceof WSColumnPreparedStatement);
        ps.close();
    }
}
