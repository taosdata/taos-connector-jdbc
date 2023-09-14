package com.taosdata.jdbc.utils;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.ws.entity.FetchReq;
import org.junit.Assert;
import org.junit.Test;

public class UInt64SerializerTest {

    @Test
    public void write() {
        FetchReq fetchReq = new FetchReq();
        fetchReq.setId(-1);
        fetchReq.setReqId(-1);
        String s = JSON.toJSONString(fetchReq);
        Assert.assertTrue(s.contains("18446744073709551615"));
        FetchReq fetchReq1 = JSON.parseObject(s, FetchReq.class);
        Assert.assertEquals(-1, fetchReq1.getId());
    }
}