package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.tmq.meta.*;
import org.junit.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WSConsumerMetaTest {
    private static final String HOST = "127.0.0.1";
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerMetaTest.class);
    private static final String SUPER_TABLE = "st";
    private static final String SUPER_TABLE_JSON = "st_json";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_ws_map" + DB_NAME, "topic_db" + DB_NAME, "topic_json" + DB_NAME};

    @Test
    public void testCreateChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("create table if not exists %s using %s.%s tags(%s, '%s', %s)", "ct" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("ct1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st");
            dstMeta.setTagNum(3);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 4, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            tags.add(new Tag("t2", 8, JsonUtil.getObjectMapper().getNodeFactory().textNode("\"t1\"")));
            tags.add(new Tag("t3", 1, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }
            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testAutoCreateChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("insert into %s using %s.%s tags(%s, '%s', %s) values (now, 1)", "act" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("act1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st");
            dstMeta.setTagNum(3);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 4, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            tags.add(new Tag("t2", 8, JsonUtil.getObjectMapper().getNodeFactory().textNode("\"t1\"")));
            tags.add(new Tag("t3", 1, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            int looptime = 10;
            boolean getCreate = false;
            while (!getCreate && looptime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                looptime--;
            }
            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testCreateChildTableJson() throws Exception {
        String topic = topics[2];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE_JSON);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("create table if not exists %s using %s.%s tags('%s')", "et" + idx, DB_NAME, SUPER_TABLE_JSON, "{\"a\":1}");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("et1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st_json");
            dstMeta.setTagNum(1);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 15, JsonUtil.getObjectMapper().getNodeFactory().textNode("{\"a\":1}")));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            int loopTime = 10;
            boolean getCreate = false;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateMultiChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        int subTableNum = 9;
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                String sql = "create table if not exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    sql += String.format("%s using %s.%s tags(%s, '%s', %s)", "dt" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                }
                statement.execute(sql);
            }

            int changeNum = subTableNum;
            int loopTime = 10;

            List<MetaCreateChildTable> metaList = new ArrayList<>();
            while (changeNum > 0 && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(MetaType.CREATE, meta.getType());
                    metaList.add(meta);
                    if (!meta.getCreateList().isEmpty()){
                        changeNum -= meta.getCreateList().size();
                    } else {
                        changeNum--;
                    }
                }
                loopTime--;
            }
            if(changeNum > 0){
                throw new Exception("not all child table created");
            }

            List<Integer> tags = new ArrayList<>();
            for (MetaCreateChildTable metaCreateChildTable : metaList){
                if (!metaCreateChildTable.getCreateList().isEmpty()){
                    for (ChildTableInfo childTableInfo : metaCreateChildTable.getCreateList()){
                        tags.add(childTableInfo.getTags().get(0).getValue().asInt());
                    }
                } else {
                    tags.add(metaCreateChildTable.getTags().get(0).getValue().asInt());
                }
            }

            tags.sort(Integer::compareTo);
            for (int i = 1; i <= subTableNum; i++){
                Assert.assertTrue(i == tags.get(i - 1));
            }
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateSuperTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                statement.execute("create stable if not exists " + SUPER_TABLE + idx
                        + " (ts timestamp, c1 int) tags(t1 int, t2 bool)");
            }

            MetaCreateSuperTable dstMeta = new MetaCreateSuperTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("st1");
            dstMeta.setTableType(TableType.SUPER);
            List<Column> columns = new ArrayList<>();
            columns.add(new Column("ts", 9, null, false, "delta-i", "lz4", "medium"));
            columns.add(new Column("c1", 4, null, false, "simple8b", "lz4", "medium"));
            dstMeta.setColumns(columns);
            List<Column> tags = new ArrayList<>();
            tags.add(new Column("t1", 4, null, null, null, null, null));
            tags.add(new Column("t2", 1, null, null, null, null, null));
            dstMeta.setTags(tags);

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateSuperTable meta = (MetaCreateSuperTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateNormalTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        String normalTable = "nn";
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                statement.execute("create table if not exists " + normalTable + idx
                        + " (ts timestamp, c1 int)");
            }

            MetaCreateNormalTable dstMeta = new MetaCreateNormalTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("nn1");
            dstMeta.setTableType(TableType.NORMAL);
            List<Column> columns = new ArrayList<>();
            columns.add(new Column("ts", 9, null, false, "delta-i", "lz4", "medium"));
            columns.add(new Column("c1", 4, null, false, "simple8b", "lz4", "medium"));
            dstMeta.setColumns(columns);
            List<Column> tags = new ArrayList<>();
            dstMeta.setTags(tags);

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateNormalTable meta = (MetaCreateNormalTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testDropSuperTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 2;
                statement.execute("create stable if not exists " + SUPER_TABLE + idx
                        + " (ts timestamp, c1 int) tags(t1 int, t2 bool)");

                statement.execute("drop stable if exists " + SUPER_TABLE + idx);
            }

            MetaDropSuperTable dstMeta = new MetaDropSuperTable();
            dstMeta.setType(MetaType.DROP);
            dstMeta.setTableName("st2");
            dstMeta.setTableType(TableType.SUPER);

            boolean getDrop = false;
            int maxLoop = 10;
            while (!getDrop && maxLoop > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta().getType() == MetaType.DROP) {
                        MetaDropSuperTable meta = (MetaDropSuperTable) r.getMeta();
                        Assert.assertEquals(dstMeta, meta);
                        getDrop = true;
                        break;
                    }
                }
                maxLoop--;
            }

            Assert.assertTrue(getDrop);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }

    @Test
    @Ignore // tsdb bug
    public void testDropChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        int subTableNum = 5;
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                String sql = "create table if not exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    sql += String.format(" %s using %s.%s tags(%s, '%s', %s)", "cht" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                }
                statement.execute(sql);

                String dropSql = "drop table if exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    dropSql += String.format(" cht" + idx + ", ");
                }

                dropSql = dropSql.substring(0, dropSql.length() - 2);
                statement.execute(dropSql);
            }

            boolean getDrop = false;
            int loopTime = 10;

            List<String> dropChildTableNameList = new ArrayList<>();
            while(!getDrop && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta().getType() == MetaType.DROP) {
                        MetaDropTable meta = (MetaDropTable) r.getMeta();
                        dropChildTableNameList.addAll(meta.getTableNameList());

                        if (dropChildTableNameList.size() == subTableNum) {
                            getDrop = true;
                            break;
                        }
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getDrop);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testAlterTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = buildConsumerProperties("grp_single_table_tags");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1001;
                String sql = String.format("create table if not exists %s using %s.%s tags(%s, '%s', %s)", "ct" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);

                String alterSql = String.format("alter table ct" + idx + " set tag t1=1001, t2='tt1001', t3=false");
                statement.execute(alterSql);
            }

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta() != null
                            && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(MetaType.ALTER, meta.getType());
                        Assert.assertEquals("", meta.getTableName());
                        Assert.assertEquals(TableType.CHILD, meta.getTableType());
                        Assert.assertEquals(19, meta.getAlterType());

                        Assert.assertNull(meta.getColName());
                        Assert.assertNull(meta.getColNewName());
                        Assert.assertEquals(0, meta.getColType());
                        Assert.assertEquals(0, meta.getColLength());
                        Assert.assertNull(meta.getColValue());
                        Assert.assertFalse(meta.isColValueNull());

                        Assert.assertNull(meta.getTags());
                        Assert.assertNull(meta.getEncode());
                        Assert.assertNull(meta.getCompress());
                        Assert.assertNull(meta.getLevel());
                        Assert.assertNull(meta.getRefDbName());
                        Assert.assertNull(meta.getRefTbName());
                        Assert.assertNull(meta.getRefColName());
                        Assert.assertNull(meta.getWhere());

                        Assert.assertNotNull(meta.getTables());
                        Assert.assertEquals(1, meta.getTables().size());

                        AlterTableTagsInfo tableInfo = meta.getTables().get(0);
                        Assert.assertEquals("ct1001", tableInfo.getTableName());
                        Assert.assertNotNull(tableInfo.getTags());
                        Assert.assertEquals(3, tableInfo.getTags().size());

                        TagAlter tag1 = tableInfo.getTags().get(0);
                        Assert.assertEquals("t1", tag1.getColName());
                        Assert.assertEquals("1001", tag1.getColValue());
                        Assert.assertFalse(tag1.isColValueNull());
                        Assert.assertNull(tag1.getRegexp());
                        Assert.assertNull(tag1.getReplacement());

                        TagAlter tag2 = tableInfo.getTags().get(1);
                        Assert.assertEquals("t2", tag2.getColName());
                        Assert.assertEquals("\"tt1001\"", tag2.getColValue());
                        Assert.assertFalse(tag2.isColValueNull());
                        Assert.assertNull(tag2.getRegexp());
                        Assert.assertNull(tag2.getReplacement());

                        TagAlter tag3 = tableInfo.getTags().get(2);
                        Assert.assertEquals("t3", tag3.getColName());
                        Assert.assertEquals("fal", tag3.getColValue());
                        Assert.assertFalse(tag3.isColValueNull());
                        Assert.assertNull(tag3.getRegexp());
                        Assert.assertNull(tag3.getReplacement());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
    }

    private Properties buildConsumerProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, groupId);
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");
        return properties;
    }

    @Test
    public void testAlterTableAddColumnWithCompress() throws Exception {
        String stName = "st_add_compress";
        String topic = "topic_add_compress_" + DB_NAME;
        statement.execute("create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int)");
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_add_compress"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            statement.execute("ALTER TABLE " + stName + " ADD COLUMN c_compress_test bigint ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium'");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(14, meta.getAlterType());
                        Assert.assertEquals("c_compress_test", meta.getColName());
                        Assert.assertNotNull(meta.getEncode());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testAlterTableUpdateColumnCompress() throws Exception {
        String stName = "st_update_compress";
        String topic = "topic_update_compress_" + DB_NAME;
        statement.execute("create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int)");
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_update_compress"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            statement.execute("ALTER TABLE " + stName + " MODIFY COLUMN c1 ENCODE 'simple8b' COMPRESS 'zstd' LEVEL 'high'");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(13, meta.getAlterType());
                        Assert.assertEquals("c1", meta.getColName());
                        Assert.assertNotNull(meta.getEncode());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testAlterMultiTableTags() throws Exception {
        String stName = "st_multi_tags";
        String topic = "topic_multi_tags_" + DB_NAME;
        statement.execute("create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_multi_tags"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create two child tables to alter
            statement.execute("create table if not exists ct_multi1 using " + stName + " tags(1, 'tianjin', true)");
            statement.execute("create table if not exists ct_multi2 using " + stName + " tags(2, 'tianjin', false)");
            consumer.poll(Duration.ofMillis(200));

            // Alter tags of two tables in one statement
            statement.execute("alter table ct_multi1 set tag t1=100 ct_multi2 set tag t1=200");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        if (meta.getAlterType() == 19 && meta.getTables() != null) {
                            Assert.assertEquals(2, meta.getTables().size());
                            getAlter = true;
                        }
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testAlterStableTagWithFilter() throws Exception {
        String stName = "st_tag_filter";
        String topic = "topic_tag_filter_" + DB_NAME;
        statement.execute("create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_tag_filter"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            statement.execute("create table if not exists ct_filter1 using " + stName + " tags(10, 'beijing', true)");
            consumer.poll(Duration.ofMillis(200));

            statement.execute("alter table using " + stName + " set tag t2='shanghai' where t1=10");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        if (meta.getAlterType() == 20) {
                            Assert.assertEquals(stName, meta.getTableName());
                            Assert.assertNotNull(meta.getTags());
                            Assert.assertFalse(meta.getTags().isEmpty());
                            Assert.assertNotNull(meta.getWhere());
                            getAlter = true;
                        }
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testAlterStableTagWithRegexp() throws Exception {
        String stName = "st_tag_regexp";
        String topic = "topic_tag_regexp_" + DB_NAME;
        statement.execute("create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(20), t3 bool)");
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_tag_regexp"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            statement.execute("create table if not exists ct_regexp1 using " + stName + " tags(20, 'tianjin', true)");
            consumer.poll(Duration.ofMillis(200));

            statement.execute("alter table using " + stName
                    + " set tag t2=REGEXP_REPLACE(t2, 'tianji[a-z]', 'zhengzhou') where t2='tianjin'");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        if (meta.getAlterType() == 20 && meta.getTags() != null) {
                            TagAlter tag = meta.getTags().get(0);
                            if (tag.getRegexp() != null) {
                                Assert.assertEquals("tianji[a-z]", tag.getRegexp());
                                Assert.assertEquals("zhengzhou", tag.getReplacement());
                                Assert.assertNotNull(meta.getWhere());
                                getAlter = true;
                            }
                        }
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testCreateVirtualNormalTable() throws Exception {
        String topic = "topic_vt_normal_" + DB_NAME;
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as database " + DB_NAME);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_vtable_normal"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create source tables
            statement.execute("create table if not exists vt_src1 (ts timestamp, c1 int)");
            statement.execute("create table if not exists vt_src2 (ts timestamp, c2 float)");
            consumer.poll(Duration.ofMillis(200));

            // Create virtual normal table
            statement.execute("create vtable if not exists vt_normal1 (ts timestamp, c1 int from vt_src1.c1, c2 float from vt_src2.c2)");

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.CREATE
                            && "vt_normal1".equals(r.getMeta().getTableName())) {
                        Assert.assertEquals(TableType.NORMAL, r.getMeta().getTableType());
                        Assert.assertEquals(Boolean.TRUE, r.getMeta().getIsVirtual());
                        getCreate = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testCreateVirtualChildTable() throws Exception {
        String topic = "topic_vt_child_" + DB_NAME;
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic " + topic + " only meta as database " + DB_NAME);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_vtable_child"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create source table and virtual super table
            statement.execute("create table if not exists vct_src1 (ts timestamp, c1 int)");
            statement.execute("create stable if not exists vstb1 (ts timestamp, c1 int) tags(t1 int) virtual 1");
            consumer.poll(Duration.ofMillis(200));

            // Create virtual child table
            statement.execute("create vtable if not exists vct1 (c1 from vct_src1.c1) using vstb1 tags(1)");

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.CREATE
                            && "vct1".equals(r.getMeta().getTableName())) {
                        Assert.assertEquals(TableType.CHILD, r.getMeta().getTableType());
                        MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                        Assert.assertNotNull(meta.getRefs());
                        Assert.assertFalse(meta.getRefs().isEmpty());
                        Assert.assertEquals("c1", meta.getRefs().get(0).getColName());
                        getCreate = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
        statement.executeUpdate("drop topic if exists " + topic);
    }

    @Test
    public void testAlterVirtualTableAddColumnWithRef() throws Exception {
        String topic = topics[1];
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_vt_add_col"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create source table and virtual normal table
            statement.execute("create table if not exists vadd_src1 (ts timestamp, c1 int, c2 bigint)");
            statement.execute("create vtable if not exists vadd_vt1 (ts timestamp, c1 int from vadd_src1.c1)");
            consumer.poll(Duration.ofMillis(200));

            // Add column with ref to virtual table
            statement.execute("alter vtable vadd_vt1 add column c2 bigint from vadd_src1.c2");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER
                            && "vadd_vt1".equals(r.getMeta().getTableName())) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(18, meta.getAlterType());
                        Assert.assertEquals("c2", meta.getColName());
                        Assert.assertNotNull(meta.getRefTbName());
                        Assert.assertNotNull(meta.getRefColName());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }

    @Ignore("TDengine server does not deliver alterType=16 (alter column ref) meta via TMQ yet")
    @Test
    public void testAlterVirtualTableAlterColumnRef() throws Exception {
        String topic = topics[1];
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_vt_alter_col"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create source tables and virtual table
            statement.execute("create table if not exists vref_src1 (ts timestamp, c1 int)");
            statement.execute("create table if not exists vref_src2 (ts timestamp, c1 int)");
            statement.execute("create vtable if not exists vref_vt1 (ts timestamp, c1 int from vref_src1.c1)");
            consumer.poll(Duration.ofMillis(200));

            // Change column ref to point to different source
            statement.execute("alter vtable vref_vt1 alter column c1 set vref_src2.c1");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER
                            && "vref_vt1".equals(r.getMeta().getTableName())) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(16, meta.getAlterType());
                        Assert.assertEquals("c1", meta.getColName());
                        Assert.assertNotNull(meta.getRefTbName());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }

    @Ignore("TDengine server does not deliver alterType=17 (set ref null) meta via TMQ yet")
    @Test
    public void testAlterVirtualTableSetRefNull() throws Exception {
        String topic = topics[1];
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(buildConsumerProperties("grp_vt_set_null"))) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));

            // Create source table and virtual table
            statement.execute("create table if not exists vnull_src1 (ts timestamp, c1 int)");
            statement.execute("create vtable if not exists vnull_vt1 (ts timestamp, c1 int from vnull_src1.c1)");
            consumer.poll(Duration.ofMillis(200));

            // Set column ref to null
            statement.execute("alter vtable vnull_vt1 alter column c1 set null");

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : records) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER
                            && "vnull_vt1".equals(r.getMeta().getTableName())) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(17, meta.getAlterType());
                        Assert.assertEquals("c1", meta.getColName());
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() +"/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        for (String topic : topics) {
            statement.executeUpdate("drop topic if exists " + topic);
        }
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
        statement.execute("create stable if not exists " + SUPER_TABLE_JSON
                + " (ts timestamp, c1 int) tags(t1 json)");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
                        statement.executeUpdate("drop topic if exists " + topic);
                    }
                    statement.executeUpdate("drop database if exists " + DB_NAME);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}
