package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {

    @Test
    public void isEmptyNull() {
        Assert.assertTrue(StringUtils.isEmpty(null));
    }

    @Test
    public void isEmptyEmpty() {
        Assert.assertTrue(StringUtils.isEmpty(""));
    }

    @Test
    public void isNumericNull() {
        Assert.assertFalse(StringUtils.isNumeric(null));
    }

    @Test
    public void isNumericEmpty() {
        Assert.assertFalse(StringUtils.isNumeric(""));
    }

    @Test
    public void isNumericStr() {
        Assert.assertFalse(StringUtils.isNumeric("abc"));
    }

    @Test
    public void isNumericNeg() {
        Assert.assertFalse(StringUtils.isNumeric("-21"));
    }

    @Test
    public void isNumericPoint() {
        Assert.assertFalse(StringUtils.isNumeric("2.15"));
    }

    @Test
    public void isNumeric() {
        Assert.assertTrue(StringUtils.isNumeric("61"));
    }

    @Test
    public void getBasicUrlTest() {
        Assert.assertEquals("jdbc:TAOS://localhost:6030/", StringUtils.getBasicUrl("jdbc:TAOS://localhost:6030/?user=root&password=taosdata"));
        Assert.assertEquals("jdbc:TAOS://localhost:6030/", StringUtils.getBasicUrl("jdbc:TAOS://localhost:6030/"));
    }

}