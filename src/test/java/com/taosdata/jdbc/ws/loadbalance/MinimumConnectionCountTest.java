package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.RebalanceUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.WSConnection;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket master slave test", author = "yjshe", version = "3.2.11")
@FixMethodOrder
public class MinimumConnectionCountTest {
    private static final String host = "127.0.0.1";
    private static final int portA = 6041;
    private static final String db_name = TestUtils.camelToSnake(MinimumConnectionCountTest.class);
    private static final String tableName = "meters";
    static  private Connection connection;

    static private TaosAdapterMock mockA;
    static private TaosAdapterMock mockB;
    static private TaosAdapterMock mockC;


    @Description("test connection count")
    @Test
    public void connectionCountTest() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + host + ":" + mockA.getListenPort()
                + "," + host + ":" + mockB.getListenPort()
                + "," + host + ":" + mockC.getListenPort() + "/?user=root&password=taosdata";

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        Connection connection1 = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)connection1).getParam();
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());

        Connection connection2 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());

        Connection connection3 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        String url2 = "jdbc:TAOS-WS://"
                + "," + host + ":" + mockA.getListenPort()
                + "," + host + ":" + mockC.getListenPort() + "/?user=root&password=taosdata";

        Connection connection4 = DriverManager.getConnection(url2, properties);
        Assert.assertEquals(2, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Connection connection5 = DriverManager.getConnection(url2, properties);
        Assert.assertEquals(2, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(2, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());
        Assert.assertEquals(1, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());

        Connection connection6 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(2, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());

        // stop one node
        mockA.stop();

        Thread.sleep(500);
        // check the connection count
        Statement statement1 = connection1.createStatement();
        ResultSet rs = statement1.executeQuery("show databases;");
        rs.close();
        statement1.close();

        Statement statement2 = connection4.createStatement();
        ResultSet rs2 = statement2.executeQuery("show databases;");
        rs2.close();
        statement2.close();

        Assert.assertEquals(3, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(3, RebalanceUtil.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        connection1.close();
        connection2.close();
        connection3.close();
        connection4.close();
        connection5.close();
        connection6.close();
        // restart mockA
        mockA.start();
    }

    @Test
    public void testConcurrentConnections() throws Exception {
       String url = "jdbc:TAOS-WS://" +
                host + ":" + mockA.getListenPort() + "," +
                host + ":" + mockB.getListenPort() + "," +
                host + ":" + mockC.getListenPort() +
                "/?user=root&password=taosdata";

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        Assert.assertNull(RebalanceUtil.getEndpointInfo(new Endpoint(host, mockA.getListenPort(), false)));
        Assert.assertNull(RebalanceUtil.getEndpointInfo(new Endpoint(host, mockB.getListenPort(), false)));
        Assert.assertNull(RebalanceUtil.getEndpointInfo(new Endpoint(host, mockC.getListenPort(), false)));

        int threadCount = 15;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Connection> connections = new ArrayList<>(); // 保存所有连接，用于后续关闭

        // 4. 创建并启动15个线程
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    Connection conn = DriverManager.getConnection(url, properties);
                    synchronized (connections) {
                        connections.add(conn);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail("线程创建连接失败：" + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(30, java.util.concurrent.TimeUnit.SECONDS);

        Assert.assertEquals("总连接数应为15", 15, connections.size());

        // 7. 获取每个节点的连接数
        ConnectionParam param = ((WSConnection)connections.get(0)).getParam();
        int countA = RebalanceUtil.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        int countB = RebalanceUtil.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount();
        int countC = RebalanceUtil.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount();

        System.out.println("并发连接后各节点连接数：A=" + countA + ", B=" + countB + ", C=" + countC);
        Assert.assertEquals(15, countA + countB + countC);
        Assert.assertEquals(5, countA);
        Assert.assertEquals(5, countB);
        Assert.assertEquals(5, countC);

        // 9. 清理资源：关闭所有连接
        for (Connection conn : connections) {
            if (conn != null) {
                conn.close();
            }
        }
    }


    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        TestUtils.runInMain();
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        String url;
        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + portA + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();

        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + "(ts timestamp, f int)");
        statement.execute("insert into " + db_name + "." + tableName + " values (now, 1)");
        statement.close();
    }

    @AfterClass
    static public void after() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + db_name);
            statement.close();
            connection.close();
        }
    }

    @Before
    public void setUp() throws IOException {
        mockA = new TaosAdapterMock();
        mockB = new TaosAdapterMock();
        mockC = new TaosAdapterMock();
        mockA.start();
        mockB.start();
        mockC.start();
    }

    @After
    public void tearDown() {
        if (mockA != null) {
            mockA.stop();
        }
        if (mockB != null) {
            mockB.stop();
        }
        if (mockC != null) {
            mockC.stop();
        }
    }
}