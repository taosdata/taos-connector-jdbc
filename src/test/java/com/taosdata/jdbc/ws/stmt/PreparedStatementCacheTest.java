package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.AbsWSPreparedStatement;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test PreparedStatement cache functionality
 */
public class PreparedStatementCacheTest {
    private final String dbName = WsStmtWriteTestSupport.dbName(PreparedStatementCacheTest.class);
    private final String tableName = "cache_test";
    private Connection connection;

    @Test
    public void testBasicCache() throws SQLException {
        // Test that same SQL reuses cached PreparedStatement
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement ps1 = connection.prepareStatement(sql);
        long instanceId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        ps1.setLong(1, System.currentTimeMillis());
        ps1.setInt(2, 1);
        ps1.executeUpdate();
        ps1.close();

        // Second prepare should return cached statement (same object)
        PreparedStatement ps2 = connection.prepareStatement(sql);
        long instanceId2 = ((AbsWSPreparedStatement) ps2).getInstanceId();

        Assert.assertEquals("Should reuse cached statement", instanceId1, instanceId2);

        ps2.setLong(1, System.currentTimeMillis());
        ps2.setInt(2, 2);
        ps2.executeUpdate();
        ps2.close();

        // Verify data
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + dbName + "." + tableName)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    public void testMultipleStatementsReuse() throws SQLException {
        // Test cache reuse for multiple cycles
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";
        PreparedStatement firstPs = connection.prepareStatement(sql);
        long firstInstanceId = ((AbsWSPreparedStatement) firstPs).getInstanceId();
        firstPs.close();

        for (int i = 0; i < 10; i++) {
            PreparedStatement ps = connection.prepareStatement(sql);
            long instanceId = ((AbsWSPreparedStatement) ps).getInstanceId();

            Assert.assertEquals("Iteration " + i + " should reuse cached statement",
                               firstInstanceId, instanceId);

            ps.setLong(1, System.currentTimeMillis());
            ps.setInt(2, i);
            ps.executeUpdate();
            ps.close();
        }

        // Verify all data inserted
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + dbName + "." + tableName)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(10, rs.getInt(1));
        }
    }

    @Test
    public void testLRUEviction() throws SQLException {
        // Test basic LRU eviction: cache size = 5, create 6 statements
        // After all 6 statements close, only last 5 should be cached
        PreparedStatement[] statements = new PreparedStatement[6];
        long[] instanceIds = new long[6];

        for (int i = 0; i < 6; i++) {
            String sql = "INSERT INTO " + dbName + ".t" + i + " VALUES(?, ?)";
            statements[i] = connection.prepareStatement(sql);
            instanceIds[i] = ((AbsWSPreparedStatement) statements[i]).getInstanceId();
            statements[i].setLong(1, System.currentTimeMillis());
            statements[i].setInt(2, i);
            statements[i].executeUpdate();
            statements[i].close();
        }
        // After all close: cache = [t1, t2, t3, t4, t5] (t0 evicted due to size limit)

        // t0 should be evicted, re-prepare should create new instance
        PreparedStatement ps0 = connection.prepareStatement("INSERT INTO " + dbName + ".t0 VALUES(?, ?)");
        long newId0 = ((AbsWSPreparedStatement) ps0).getInstanceId();
        Assert.assertNotEquals("t0 should be evicted", instanceIds[0], newId0);
        ps0.close();

        // t1-t5 should have been cached initially, but after ps0 closes,
        // t1 gets evicted to make room for new t0
        // So now cache = [t2, t3, t4, t5, t0']

        // t1 should now be evicted
        PreparedStatement ps1 = connection.prepareStatement("INSERT INTO " + dbName + ".t1 VALUES(?, ?)");
        long newId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        Assert.assertNotEquals("t1 should be evicted", instanceIds[1], newId1);
        ps1.close();
    }

    @Test
    public void testUseDatabaseIsolation() throws SQLException {
        // Create two databases
        String db2 = dbName + "_2";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + db2);
            stmt.execute("CREATE DATABASE " + db2);
            stmt.execute("CREATE TABLE " + db2 + "." + tableName + " (ts TIMESTAMP, v INT)");
        }

        try {
            // Prepare statement in db1
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE " + dbName);
            }
            String sql = "INSERT INTO " + tableName + " VALUES(?, ?)";
            PreparedStatement ps1 = connection.prepareStatement(sql);
            long instanceId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
            ps1.close();

            // Switch to db2 and prepare same SQL
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE " + db2);
            }
            PreparedStatement ps2 = connection.prepareStatement(sql);
            long instanceId2 = ((AbsWSPreparedStatement) ps2).getInstanceId();
            ps2.close();

            // Should be different instances (different database context)
            Assert.assertNotEquals("Different databases should use different cached statements",
                                  instanceId1, instanceId2);
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP DATABASE IF EXISTS " + db2);
            }
        }
    }

    @Test
    public void testConcurrentUseSameSQL() throws SQLException {
        // Test that same SQL with concurrent use creates new statement
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement ps1 = connection.prepareStatement(sql);
        long instanceId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        Assert.assertTrue("First statement should be marked inUse",
                         ((AbsWSPreparedStatement) ps1).isInUse());

        // While ps1 is still open (inUse=true), prepare again
        PreparedStatement ps2 = connection.prepareStatement(sql);
        long instanceId2 = ((AbsWSPreparedStatement) ps2).getInstanceId();

        // Should be different instances
        Assert.assertNotEquals("Concurrent use should create new statement",
                              instanceId1, instanceId2);

        ps1.close();
        ps2.close();
    }

    @Test
    public void testCacheDisabled() throws SQLException {
        connection.close();

        // Open connection with cache disabled
        connection = openWebSocketConnectionWithCacheSize(0);
        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + dbName + "." + tableName + " (ts TIMESTAMP, v INT)");
        }

        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        PreparedStatement ps1 = connection.prepareStatement(sql);
        long instanceId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(sql);
        long instanceId2 = ((AbsWSPreparedStatement) ps2).getInstanceId();
        ps2.close();

        // Should be different instances (cache disabled)
        Assert.assertNotEquals("Cache disabled should create new statements",
                              instanceId1, instanceId2);
    }

    @Test
    public void testOnlyCacheInsertStatements() throws SQLException {
        // Insert statement should be cached
        String insertSql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";
        PreparedStatement ps1 = connection.prepareStatement(insertSql);
        long insertId1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(insertSql);
        long insertId2 = ((AbsWSPreparedStatement) ps2).getInstanceId();
        ps2.close();

        Assert.assertEquals("Insert should be cached", insertId1, insertId2);

        // Query statement should NOT be cached (always create new)
        String querySql = "SELECT * FROM " + dbName + "." + tableName + " WHERE ts > ?";
        PreparedStatement qs1 = connection.prepareStatement(querySql);
        long queryId1 = ((AbsWSPreparedStatement) qs1).getInstanceId();
        qs1.close();

        PreparedStatement qs2 = connection.prepareStatement(querySql);
        long queryId2 = ((AbsWSPreparedStatement) qs2).getInstanceId();
        qs2.close();

        Assert.assertNotEquals("Query should not be cached", queryId1, queryId2);
    }

    @Test
    public void testConnectionCloseReleasesCache() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        // Fill cache
        for (int i = 0; i < 3; i++) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1, System.currentTimeMillis());
            ps.setInt(2, i);
            ps.executeUpdate();
            ps.close();
        }

        // Close connection (should release all cached statements)
        connection.close();

        // Reopen and test
        connection = WsStmtWriteTestSupport.openWebSocketConnection("traditional", false);

        PreparedStatement ps = connection.prepareStatement(sql);
        // Should succeed (no exceptions)
        ps.close();
    }

    @Test
    public void testSameKeyDifferentStatement() throws SQLException {
        // First statement closes and caches
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";
        PreparedStatement ps1 = connection.prepareStatement(sql);
        long id1 = ((AbsWSPreparedStatement) ps1).getInstanceId();
        ps1.close();

        // Open two more with same SQL
        PreparedStatement ps2 = connection.prepareStatement(sql);
        PreparedStatement ps3 = connection.prepareStatement(sql);

        // ps2 should get cached one (ps1)
        long id2 = ((AbsWSPreparedStatement) ps2).getInstanceId();
        Assert.assertEquals("ps2 should reuse ps1", id1, id2);

        // ps3 should be new (ps2 is in use)
        long id3 = ((AbsWSPreparedStatement) ps3).getInstanceId();
        Assert.assertNotEquals("ps3 should be new", id2, id3);

        // Close ps3 first (cache already has ps1, ps3 should be released)
        ps3.close();

        // Close ps2 (ps2 is ps1, just mark idle)
        ps2.close();

        // Next prepare should get ps1/ps2 from cache (not ps3, which was released)
        PreparedStatement ps4 = connection.prepareStatement(sql);
        long id4 = ((AbsWSPreparedStatement) ps4).getInstanceId();
        Assert.assertEquals("ps4 should reuse ps1/ps2", id1, id4);
        ps4.close();
    }

    @Test
    public void testCacheStateReset() throws SQLException {
        String sql = "INSERT INTO " + dbName + "." + tableName + " VALUES(?, ?)";

        // Use statement and set parameters
        PreparedStatement ps1 = connection.prepareStatement(sql);
        ps1.setLong(1, 1000L);
        ps1.setInt(2, 100);
        ps1.executeUpdate();

        // Close and reopen (should reset parameters)
        ps1.close();

        PreparedStatement ps2 = connection.prepareStatement(sql);
        // Should be same cached instance
        Assert.assertEquals(((AbsWSPreparedStatement) ps1).getInstanceId(),
                           ((AbsWSPreparedStatement) ps2).getInstanceId());

        // Set new parameters (old ones should be cleared)
        ps2.setLong(1, 2000L);
        ps2.setInt(2, 200);
        int rows = ps2.executeUpdate();

        Assert.assertEquals("Should insert one row", 1, rows);
        ps2.close();

        // Verify both rows inserted
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + dbName + "." + tableName)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
        }
    }

    private Connection openWebSocketConnectionWithCacheSize(int cacheSize) throws SQLException {
        String url = WsStmtWriteTestSupport.traditionalWebSocketUrl();
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_STMT_CACHE_SIZE, String.valueOf(cacheSize));
        return DriverManager.getConnection(url, props);
    }

    @Before
    public void before() throws SQLException {
        connection = WsStmtWriteTestSupport.openWebSocketConnection("traditional", false);
        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);

        // Create main test table
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + dbName + "." + tableName + " (ts TIMESTAMP, v INT)");
        }

        // Create tables for LRU test
        for (int i = 0; i < 6; i++) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + dbName + ".t" + i + " (ts TIMESTAMP, v INT)");
            }
        }
    }

    @After
    public void after() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            WsStmtWriteTestSupport.dropDatabase(connection, dbName);
            connection.close();
        }
    }

    @BeforeClass
    public static void setUp() {
        TestUtils.runInMain();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
    }
}
