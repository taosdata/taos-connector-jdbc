package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ReqIdTest {

    @Test
    public void testMurmurHash32() {
        long i = ReqId.murmurHash32("driver-go".getBytes(StandardCharsets.UTF_8), 0);

        Assert.assertEquals(3037880692L, i);
    }

    @Test
    public void testGetReqID() {
        System.out.println(ReqId.getReqID());
    }
}