package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.ws.entity.Request;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@TestTarget(alias = "consumer request content", author = "huolibo", version = "3.1.0")
@RunWith(CatalogRunner.class)
public class TMQRequestFactoryTest {
    private static TMQRequestFactory factory;

    @Test
    @Description("Generate Subscribe")
    public void testGenerateSubscribe() {
        String[] topics = {"topic_1", "topic_2"};
        Request request = factory.generateSubscribe("root", "taosdata", "test", "gId",
                "cId", "offset", topics
                , null, null, null);
        JSONObject jsonObject = JSONObject.parseObject(request.toString());
        SubscribeReq req = JSON.toJavaObject((JSON) JSON.toJSON(jsonObject.get("args")), SubscribeReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals("root", req.getUser());
        Assert.assertEquals("taosdata", req.getPassword());
        Assert.assertEquals("test", req.getDb());
        Assert.assertEquals("gId", req.getGroupId());
        Assert.assertEquals("cId", req.getClientId());
        Assert.assertEquals("offset", req.getOffsetRest());
        Assert.assertEquals("topic_2", req.getTopics()[1]);
    }

    @Test
    @Description("Generate Poll")
    public void testGeneratePoll() {
        Request request = factory.generatePoll(1000);
        JSONObject jsonObject = JSONObject.parseObject(request.toString());
        PollReq req = JSON.toJavaObject((JSON) JSON.toJSON(jsonObject.get("args")), PollReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getBlockingTime());
    }

    @Test
    @Description("Generate Fetch")
    public void testGenerateFetch() {
        Request request = factory.generateFetch(1_000);
        JSONObject jsonObject = JSONObject.parseObject(request.toString());
        FetchReq req = JSON.toJavaObject((JSON) JSON.toJSON(jsonObject.get("args")), FetchReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getMessageId());
    }

    @Test
    @Description("Generate Fetch Block")
    public void testGenerateFetchBlock() {
        Request request = factory.generateFetchBlock(10, 1000);
        JSONObject jsonObject = JSONObject.parseObject(request.toString());
        FetchBlockReq req = JSON.toJavaObject((JSON) JSON.toJSON(jsonObject.get("args")), FetchBlockReq.class);
        Assert.assertEquals(10, req.getReqId());
        Assert.assertEquals(1000, req.getMessageId());
    }

    @Test
    @Description("Generate Commit")
    public void testGenerateCommit() {
        Request request = factory.generateCommit(1000);
        JSONObject jsonObject = JSONObject.parseObject(request.toString());
        CommitReq req = JSON.toJavaObject((JSON) JSON.toJSON(jsonObject.get("args")), CommitReq.class);
        Assert.assertEquals(1, req.getReqId());
        Assert.assertEquals(1000, req.getMessageId());
    }

    @BeforeClass
    public static void beforeClass() {
        factory = new TMQRequestFactory();
    }
}