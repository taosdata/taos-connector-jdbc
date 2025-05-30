package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.ws.entity.Request;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.Properties;

@TestTarget(alias = "consumer request content", author = "huolibo", version = "3.1.0")
@RunWith(CatalogRunner.class)
public class TMQRequestFactoryTest {
    private static TMQRequestFactory factory;
    private static final ObjectMapper objectMapper = JsonUtil.getObjectMapper();

    @Test
    @Description("Generate Subscribe")
    public void testGenerateSubscribe() throws JsonProcessingException, SQLException {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, "test");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Shanghai");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, "app_name");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, "192.168.1.1");

        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "test");

        properties.setProperty(TMQConstants.GROUP_ID, "gId");
        properties.setProperty(TMQConstants.CLIENT_ID, "cId");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "offset");


        ConsumerParam param = new ConsumerParam(properties);

        String[] topics = {"topic_1", "topic_2"};
        Request request = factory.generateSubscribe(param, topics
                , null);
        JsonNode jsonObject = objectMapper.readTree(request.toString());
        SubscribeReq req = objectMapper.treeToValue(jsonObject.get("args"), SubscribeReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals("root", req.getUser());
        Assert.assertEquals("taosdata", req.getPassword());
        Assert.assertEquals("test", req.getDb());
        Assert.assertEquals("gId", req.getGroupId());
        Assert.assertEquals("cId", req.getClientId());
        Assert.assertEquals("offset", req.getOffsetRest());
        Assert.assertEquals("topic_2", req.getTopics()[1]);

        Assert.assertEquals("Asia/Shanghai", req.getTz());
        Assert.assertEquals("app_name", req.getApp());
        Assert.assertEquals("192.168.1.1", req.getIp());
    }

    @Test
    @Description("Generate Poll")
    public void testGeneratePoll() throws JsonProcessingException {
        Request request = factory.generatePoll(0, 1000);
        JsonNode jsonObject = objectMapper.readTree(request.toString());
        PollReq req = objectMapper.treeToValue(jsonObject.get("args"), PollReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getBlockingTime());
    }

    @Test
    @Description("Generate FetchRaw")
    public void testGenerateFetchRaw() throws JsonProcessingException {
        Request request = factory.generateFetchRaw(1_000);
        JsonNode jsonObject = objectMapper.readTree(request.toString());
        FetchRawReq req = objectMapper.treeToValue(jsonObject.get("args"), FetchRawReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getMessageId());
    }
    @Test
    @Description("Generate Commit")
    public void testGenerateCommit() throws JsonProcessingException {
        Request request = factory.generateCommit(1000);
        JsonNode jsonObject = objectMapper.readTree(request.toString());
        CommitReq req = objectMapper.treeToValue(jsonObject.get("args"), CommitReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getMessageId());
    }

    @BeforeClass
    public static void beforeClass() {
        factory = new TMQRequestFactory();
    }
}