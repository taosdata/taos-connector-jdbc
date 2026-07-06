package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.tmq.meta.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Tests that only run in main CI environment. These tests may fail in development environments due
 * to: - TDengine version differences - SQL syntax variations - Metadata format changes
 */
public class WSConsumerMetaMainTest {
  private static final String HOST = "127.0.0.1";
  private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerMetaMainTest.class);
  private static final String SUPER_TABLE = "st";
  private static final String SUPER_TABLE_JSON = "st_json";
  private static Connection connection;
  private static Statement statement;
  private static final String[] topics = {
    "topic_ws_map" + DB_NAME, "topic_db" + DB_NAME, "topic_json" + DB_NAME
  };

  @Test
  public void testAlterTable() throws Exception {
    String topic = topics[0];
    // create topic
    statement.executeUpdate(
        "create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

    Properties properties = buildConsumerProperties("grp_single_table_tags");

    try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));
      {
        int idx = 1001;
        String sql =
            String.format(
                "create table if not exists %s using %s.%s tags(%s, '%s', %s)",
                "ct" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
        statement.execute(sql);

        String alterSql = "alter table ct" + idx + " set tag t1=1001, t2='tt1001', t3=false";
        statement.execute(alterSql);
      }

      boolean getAlter = false;
      int loopTime = 10;
      while (!getAlter && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> consumerRecords =
            consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
          if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
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
    properties.setProperty(
        TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
    properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
    properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
    properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
    properties.setProperty(TMQConstants.GROUP_ID, groupId);
    properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
    properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");
    return properties;
  }

  @Test
  public void testAlterMultiTableTags() throws Exception {
    String stName = "st_multi_tags";
    String topic = "topic_multi_tags_" + DB_NAME;
    statement.execute(
        "create stable if not exists "
            + stName
            + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_multi_tags"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create two child tables to alter
      statement.execute(
          "create table if not exists ct_multi1 using " + stName + " tags(1, 'tianjin', true)");
      statement.execute(
          "create table if not exists ct_multi2 using " + stName + " tags(2, 'tianjin', false)");
      consumer.poll(Duration.ofMillis(200));

      // Alter tags of two tables in one statement
      statement.execute("alter table ct_multi1 set tag t1=100 ct_multi2 set tag t1=200");

      int loopTime = 10;
      int getAlterCount = 0;
      while (getAlterCount < 2 && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null && r.getMeta().getType() == MetaType.ALTER) {
            MetaAlterTable meta = (MetaAlterTable) r.getMeta();
            if (meta.getAlterType() == 19 && meta.getTables() != null) {
              getAlterCount++;
            }
          }
        }
        loopTime--;
      }
      Assert.assertEquals(2, getAlterCount);
      consumer.unsubscribe();
    }
    statement.executeUpdate("drop topic if exists " + topic);
  }

  @Test
  public void testAlterStableTagWithFilter() throws Exception {
    String stName = "st_tag_filter";
    String topic = "topic_tag_filter_" + DB_NAME;
    statement.execute(
        "create stable if not exists "
            + stName
            + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_tag_filter"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      statement.execute(
          "create table if not exists ct_filter1 using " + stName + " tags(10, 'beijing', true)");
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
    statement.execute(
        "create stable if not exists "
            + stName
            + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(20), t3 bool)");
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_tag_regexp"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      statement.execute(
          "create table if not exists ct_regexp1 using " + stName + " tags(20, 'tianjin', true)");
      consumer.poll(Duration.ofMillis(200));

      statement.execute(
          "alter table using "
              + stName
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

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_vtable_normal"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create source tables
      statement.execute("create table if not exists vt_src1 (ts timestamp, c1 int)");
      statement.execute("create table if not exists vt_src2 (ts timestamp, c2 float)");
      consumer.poll(Duration.ofMillis(200));

      // Create virtual normal table
      statement.execute(
          "create vtable if not exists vt_normal1 (ts timestamp, c1 int from vt_src1.c1, c2 float from vt_src2.c2)");

      boolean getCreate = false;
      int loopTime = 10;
      while (!getCreate && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null
              && r.getMeta().getType() == MetaType.CREATE
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

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_vtable_child"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create source table and virtual super table
      statement.execute("create table if not exists vct_src1 (ts timestamp, c1 int)");
      statement.execute(
          "create stable if not exists vstb1 (ts timestamp, c1 int) tags(t1 int) virtual 1");
      consumer.poll(Duration.ofMillis(200));

      // Create virtual child table
      statement.execute(
          "create vtable if not exists vct1 (c1 from vct_src1.c1) using vstb1 tags(1)");

      boolean getCreate = false;
      int loopTime = 10;
      while (!getCreate && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null
              && r.getMeta().getType() == MetaType.CREATE
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
    statement.executeUpdate(
        "create topic if not exists " + topic + " only meta as database " + DB_NAME);

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_vt_add_col"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create source table and virtual normal table
      statement.execute("create table if not exists vadd_src1 (ts timestamp, c1 int, c2 bigint)");
      statement.execute(
          "create vtable if not exists vadd_vt1 (ts timestamp, c1 int from vadd_src1.c1)");
      consumer.poll(Duration.ofMillis(200));

      // Add column with ref to virtual table
      statement.execute("alter vtable vadd_vt1 add column c2 bigint from vadd_src1.c2");

      boolean getAlter = false;
      int loopTime = 10;
      while (!getAlter && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null
              && r.getMeta().getType() == MetaType.ALTER
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


  @Test
  public void testAlterTableAddColumnWithCompress() throws Exception {
    String stName = "st_add_compress";
    String topic = "topic_add_compress_" + DB_NAME;
    statement.execute(
            "create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int)");
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

    try (TaosConsumer<Map<String, Object>> consumer =
                 new TaosConsumer<>(buildConsumerProperties("grp_add_compress"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      statement.execute(
              "ALTER TABLE "
                      + stName
                      + " ADD COLUMN c_compress_test bigint ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium'");

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
    statement.execute(
            "create stable if not exists " + stName + " (ts timestamp, c1 int) tags(t1 int)");
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " only meta as STABLE " + stName);

    try (TaosConsumer<Map<String, Object>> consumer =
                 new TaosConsumer<>(buildConsumerProperties("grp_update_compress"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      statement.execute(
              "ALTER TABLE "
                      + stName
                      + " MODIFY COLUMN c1 ENCODE 'simple8b' COMPRESS 'zstd' LEVEL 'high'");

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
  public void testAlterVirtualTableAlterColumnRef() throws Exception {
    String topic = topics[1];
    statement.executeUpdate(
            "create topic if not exists " + topic + " only meta as database " + DB_NAME);

    try (TaosConsumer<Map<String, Object>> consumer =
                 new TaosConsumer<>(buildConsumerProperties("grp_vt_alter_col"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create source tables and virtual table
      statement.execute("create table if not exists vref_src1 (ts timestamp, c1 int)");
      statement.execute("create table if not exists vref_src2 (ts timestamp, c1 int)");
      statement.execute(
              "create vtable if not exists vref_vt1 (ts timestamp, c1 int from vref_src1.c1)");
      consumer.poll(Duration.ofMillis(200));

      // Change column ref to point to different source
      statement.execute("alter vtable vref_vt1 alter column c1 set vref_src2.c1");

      boolean getAlter = false;
      int loopTime = 10;
      while (!getAlter && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null
                  && r.getMeta().getType() == MetaType.ALTER
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

  @Test
  public void testAlterVirtualTableSetRefNull() throws Exception {
    String topic = topics[1];
    statement.executeUpdate(
            "create topic if not exists " + topic + " only meta as database " + DB_NAME);

    try (TaosConsumer<Map<String, Object>> consumer =
                 new TaosConsumer<>(buildConsumerProperties("grp_vt_set_null"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      // Create source table and virtual table
      statement.execute("create table if not exists vnull_src1 (ts timestamp, c1 int)");
      statement.execute(
              "create vtable if not exists vnull_vt1 (ts timestamp, c1 int from vnull_src1.c1)");
      consumer.poll(Duration.ofMillis(200));

      // Set column ref to null
      statement.execute("alter vtable vnull_vt1 alter column c1 set null");

      boolean getAlter = false;
      int loopTime = 10;
      while (!getAlter && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() != null
                  && r.getMeta().getType() == MetaType.ALTER
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

  @Test
  public void testCreateAndAlterBaseOn() throws Exception {
    // VST inheritance (BASE ON) over the WS path: create parent VSTs + an inherited
    // child + ALTER ADD/DROP BASE ON, and verify the WS json-meta carries the frozen
    // contract through taosAdapter into the MetaCreateSuperTable / MetaAlterTable POJOs.
    String topic = "topic_base_on_" + DB_NAME;
    statement.executeUpdate("drop topic if exists " + topic);
    statement.executeUpdate("create topic " + topic + " with meta as database " + DB_NAME);

    try (TaosConsumer<Map<String, Object>> consumer =
        new TaosConsumer<>(buildConsumerProperties("grp_base_on"))) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(100));

      statement.execute(
          "create stable if not exists bo_p_device (ts timestamp, status int, temp float) "
              + "tags (region int, site binary(32)) virtual 1");
      statement.execute(
          "create stable if not exists bo_p_metric (ts timestamp, val double) "
              + "tags (unit nchar(8)) virtual 1");
      statement.execute(
          "create stable if not exists bo_leaf (ts timestamp, accuracy int) tags (sensor_id int) "
              + "base on "
              + DB_NAME
              + ".bo_p_device, "
              + DB_NAME
              + ".bo_p_metric virtual 1");
      statement.execute(
          "create stable if not exists bo_standalone (ts timestamp, own_col int) "
              + "tags (own_tag int) virtual 1");
      statement.execute(
          "alter stable " + DB_NAME + ".bo_standalone add base on " + DB_NAME + ".bo_p_metric");
      statement.execute(
          "alter stable " + DB_NAME + ".bo_standalone drop base on " + DB_NAME + ".bo_p_metric");

      boolean gotCreateBaseOn = false;
      boolean gotCreateStandaloneNoBaseOn = false;
      boolean gotAlterAdd = false;
      boolean gotAlterDrop = false;
      int loopTime = 20;
      while ((!gotCreateBaseOn || !gotCreateStandaloneNoBaseOn || !gotAlterAdd || !gotAlterDrop)
          && loopTime > 0) {
        ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(200));
        for (ConsumerRecord<Map<String, Object>> r : records) {
          if (r.getMeta() == null) continue;
          if (r.getMeta().getType() == MetaType.CREATE
              && "bo_leaf".equals(r.getMeta().getTableName())
              && r.getMeta() instanceof MetaCreateSuperTable) {
            MetaCreateSuperTable meta = (MetaCreateSuperTable) r.getMeta();
            Assert.assertNotNull("bo_leaf must carry baseOn", meta.getBaseOn());
            Assert.assertEquals(2, meta.getBaseOn().size());
            Assert.assertTrue(meta.getBaseOn().contains("bo_p_device"));
            Assert.assertTrue(meta.getBaseOn().contains("bo_p_metric"));
            Assert.assertNotNull(meta.getOwnColStart());
            Assert.assertNotNull(meta.getOwnTagStart());
            Assert.assertTrue(meta.getOwnColStart() > 0);
            Assert.assertTrue(meta.getOwnTagStart() > 0);
            gotCreateBaseOn = true;
          }
          if (r.getMeta().getType() == MetaType.CREATE
              && "bo_standalone".equals(r.getMeta().getTableName())
              && r.getMeta() instanceof MetaCreateSuperTable) {
            // non-inherited stable: baseOn must be null/absent (old-consumer compat)
            MetaCreateSuperTable meta = (MetaCreateSuperTable) r.getMeta();
            Assert.assertTrue(
                "bo_standalone must NOT carry baseOn",
                meta.getBaseOn() == null || meta.getBaseOn().isEmpty());
            gotCreateStandaloneNoBaseOn = true;
          }
          if (r.getMeta().getType() == MetaType.ALTER
              && r.getMeta() instanceof MetaAlterTable) {
            MetaAlterTable meta = (MetaAlterTable) r.getMeta();
            if (meta.getAlterType() == AlterType.ADD_BASE_ON.getValue()) {
              Assert.assertNotNull(meta.getBaseOn());
              Assert.assertTrue(meta.getBaseOn().contains("bo_p_metric"));
              gotAlterAdd = true;
            } else if (meta.getAlterType() == AlterType.DROP_BASE_ON.getValue()) {
              Assert.assertNotNull(meta.getBaseOn());
              Assert.assertTrue(meta.getBaseOn().contains("bo_p_metric"));
              gotAlterDrop = true;
            }
          }
        }
        loopTime--;
      }
      Assert.assertTrue("missing CREATE bo_leaf with baseOn", gotCreateBaseOn);
      Assert.assertTrue("missing CREATE bo_standalone without baseOn", gotCreateStandaloneNoBaseOn);
      Assert.assertTrue("missing ALTER ADD BASE ON (22)", gotAlterAdd);
      Assert.assertTrue("missing ALTER DROP BASE ON (23)", gotAlterDrop);
      consumer.commitSync();
      consumer.unsubscribe();
    }
    statement.executeUpdate("drop topic if exists " + topic);
  }

  @BeforeClass
  public static void before() throws SQLException {
    TestUtils.runInMain();
    String url = SpecifyAddress.getInstance().getRestUrl();
    if (url == null) {
      url =
          "jdbc:TAOS-WS://"
              + HOST
              + ":"
              + TestEnvUtil.getWsPort()
              + "/?user="
              + TestEnvUtil.getUser()
              + "&password="
              + TestEnvUtil.getPassword();
    }
    Properties properties = new Properties();
    properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
    properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
    connection = DriverManager.getConnection(url, properties);
    statement = connection.createStatement();
    for (String topic : topics) {
      statement.executeUpdate("drop topic if exists " + topic);
    }

    TestUtils.waitTransactionDone(connection);
    statement.execute("drop database if exists " + DB_NAME);
    statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
    statement.execute("use " + DB_NAME);
    statement.execute(
        "create stable if not exists "
            + SUPER_TABLE
            + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
    statement.execute(
        "create stable if not exists "
            + SUPER_TABLE_JSON
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
