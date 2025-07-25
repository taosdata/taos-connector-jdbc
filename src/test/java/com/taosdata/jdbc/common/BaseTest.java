package com.taosdata.jdbc.common;

import org.junit.BeforeClass;

import java.util.TimeZone;

public class BaseTest {

    // 保存原始时区
    private static final TimeZone originalTimeZone = TimeZone.getDefault();
    static {
        System.out.println("originalTimeZone == " + originalTimeZone);
    }

    @BeforeClass
    public static void setUpClass() {
        // 保存原始时区
        TimeZone.setDefault(originalTimeZone);
//        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
}