package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBPreparedStatement;
import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

@Ignore
public class PrepareStatementBenchTest extends BaseTest {
    static String host = "127.0.0.1";
//    static String host = "192.168.1.98";
    static String db_name = TestUtils.camelToSnake(PrepareStatementBenchTest.class);

    @Test
    public void testJni() throws SQLException {
        String sql = "insert into ? using " + db_name + ".wpt_jni"
                + " tags (?, ?, ?, ?, ?, ?, ?) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
//        statement.execute("drop database if exists " + db_name);
//        statement.execute("create database " + db_name + " keep 36500");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + ".wpt_jni" +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double , c7 bool, c8 varchar(20), c9 nchar(20), c10 binary(20)) " +
                "tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 bool)");
        statement.close();

        TSDBPreparedStatement preparedStatement = (TSDBPreparedStatement) connection.prepareStatement(sql);
        long start = System.currentTimeMillis();
        long timeout = 0;
        for (int i = 0; i < 100; i++) {
            preparedStatement.setTableName("prepare_bench" + ".jni" + i);
            preparedStatement.setTagByte(0, (byte) i);
            preparedStatement.setTagShort(1, (short) (i * 10));
            preparedStatement.setTagInt(2, i * 100);
            preparedStatement.setTagLong(3, i * 1000L);
            preparedStatement.setTagFloat(4, i * 1.0f);
            preparedStatement.setTagDouble(5, i * 1000.0);
            preparedStatement.setTagBoolean(6, true);

            int times = 1000;
            long time = System.currentTimeMillis();
            ArrayList<Long> list = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                list.add(time + i * 1000 + i1);
            }
            preparedStatement.setTimestamp(0, list);

            ArrayList<Byte> bytes = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                bytes.add((byte) i1);
            }
            preparedStatement.setByte(1, bytes);

            ArrayList<Short> shorts = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                shorts.add((short) (i1 * 10));
            }
            preparedStatement.setShort(2, shorts);

            ArrayList<Integer> ints = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                ints.add(i1 * 100);
            }
            preparedStatement.setInt(3, ints);

            ArrayList<Long> longs = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                longs.add(i1 * 1000L);
            }
            preparedStatement.setLong(4, longs);

            ArrayList<Float> floats = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                floats.add(i1 * 1.0f);
            }
            preparedStatement.setFloat(5, floats);

            ArrayList<Double> doubles = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                doubles.add(i1 * 1000.0);
            }
            preparedStatement.setDouble(6, doubles);

            ArrayList<Boolean> bools = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                bools.add(true);
            }
            preparedStatement.setBoolean(7, bools);

            ArrayList<String> strings = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                strings.add("abc");
            }
            preparedStatement.setString(8, strings, 20);

            ArrayList<String> nchars = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                nchars.add("世界" + i1);
            }
            preparedStatement.setNString(9, nchars, 20);

            ArrayList<String> list1 = new ArrayList<>();
            for (int i1 = 0; i1 < times; i1++) {
                list1.add("你好" + i1);
            }
            preparedStatement.setString(10, list1, 20);

            long a = System.currentTimeMillis();
            preparedStatement.columnDataAddBatch();
            preparedStatement.columnDataExecuteBatch();
            timeout += System.currentTimeMillis() - a;

        }
        long end = System.currentTimeMillis();
        System.out.println("100次总耗时：" + (end - start) + "ms" + "，每1000条平均耗时：" + (end - start) / 100 + "ms");
        System.out.println("executeBatch 执行耗时" + timeout + "ms");
        preparedStatement.close();
        connection.close();
    }

    @Test
    public void testWs() throws SQLException {
        String sql = "insert into ? using " + db_name + ".wpt_ws"
                + " tags (?, ?, ?, ?, ?, ?, ?) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/prepare_bench?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
//        statement.execute("drop database if exists " + db_name);
//        statement.execute("create database " + db_name + " keep 36500");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + ".wpt_ws" +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double , c7 bool, c8 varchar(20), c9 nchar(20), c10 binary(20)) " +
                "tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 bool)");
        statement.close();


        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement(sql);
        long start = System.currentTimeMillis();
        long timeout = 0;
        for (int i = 0; i < 100; i++) {
            preparedStatement.setTableName("prepare_bench" + ".ws" + i);
            preparedStatement.setTagByte(1, (byte) i);
            preparedStatement.setTagShort(2, (short) (i * 10));
            preparedStatement.setTagInt(3, i * 100);
            preparedStatement.setTagLong(4, i * 1000L);
            preparedStatement.setTagFloat(5, i * 1.0f);
            preparedStatement.setTagDouble(6, i * 1000.0);
            preparedStatement.setTagBoolean(7, true);
            long current = System.currentTimeMillis();
            for (int i1 = 0; i1 < 1000; i1++) {
                preparedStatement.setTimestamp(1, new Timestamp(current + i * 1000 + i1));
                preparedStatement.setByte(2, (byte) i1);
                preparedStatement.setShort(3, (short) (i1 * 10));
                preparedStatement.setInt(4, i1 * 100);
                preparedStatement.setLong(5, i1 * 1000L);
                preparedStatement.setFloat(6, i1 * 1.0f);
                preparedStatement.setDouble(7, i1 * 1000.0);
                preparedStatement.setBoolean(8, true);
                preparedStatement.setBytes(9, "abc".getBytes());
                preparedStatement.setNString(10, "世界" + i1);
                preparedStatement.setString(11, "你好" + i1);
                preparedStatement.addBatch();
            }
            long a = System.currentTimeMillis();
            preparedStatement.executeBatch();
            timeout += System.currentTimeMillis() - a;
        }

        long end = System.currentTimeMillis();
        System.out.println("100次总耗时：" + (end - start) + "ms" + "，每1000条平均耗时：" + (end - start) / 100 + "ms");
        System.out.println("executeBatch 执行耗时" + timeout + "ms");
        preparedStatement.close();
        connection.close();
    }

    @Test
    public void testWsStatement() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/prepare_bench?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + ".wpt_ws_statement" +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double , c7 bool, c8 varchar(20), c9 nchar(20), c10 binary(20)) " +
                "tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 bool)");

        long timeout = 0;
        long current = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String sql = "insert into prepare_bench.ws_statement" + i +" using " + db_name + ".wpt_ws_statement"
                    + " tags (1 , "
                    + 10 + ", "
                    +  100 + ", " +
                    i * 1000 + ", " +
                    1.0 + " , " +
                    i * 1000.0 + ", true" +
                    ") values(" + (current + i) + ", 1, "
                    + 10 + ", "
                    + 100 + ", "
                    + i * 1000 + ", "
                    + 1.0 + ", "
                    + i * 1000.0 + ", true, 'abc', '世界', '你好')";

            statement.execute(sql);
        }

        long end = System.currentTimeMillis();
        System.out.println("1000条总耗时：" + (end - current) + "ms" + "，每条平均耗时：" + (end - current) / 1000 + "ms");
        statement.close();
        connection.close();
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name + " keep 36500");
        statement.close();
        connection.close();
    }
}
