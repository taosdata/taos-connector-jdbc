package com.taosdata.jdbc.common;

import org.junit.rules.ExternalResource;
        import java.util.TimeZone;

public class TimeZoneResetRule extends ExternalResource {

    private TimeZone originalTimeZone;

    @Override
    protected void before() throws Throwable {
        // 保存原始时区
        originalTimeZone = TimeZone.getDefault();

        // 重置为系统默认时区
        TimeZone.setDefault(TimeZone.getTimeZone(System.getProperty("user.timezone")));
    }

    @Override
    protected void after() {
        // 恢复为原始时区（可选）
        TimeZone.setDefault(originalTimeZone);
    }
}