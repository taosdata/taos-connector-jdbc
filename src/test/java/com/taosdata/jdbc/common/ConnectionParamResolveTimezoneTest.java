package com.taosdata.jdbc.common;

import org.junit.Assume;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ConnectionParam#resolveTimezone(String)}
 */
public class ConnectionParamResolveTimezoneTest {

    @Test
    public void testValidIanaName() throws SQLException {
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        if (ZoneId.systemDefault().equals(tokyo)) {
            assertNull(ConnectionParam.resolveTimezone("Asia/Tokyo"));
        } else {
            assertEquals(tokyo, ConnectionParam.resolveTimezone("Asia/Tokyo"));
        }
    }

    @Test
    public void testSystemDefaultResolvesToNull() throws SQLException {
        String systemDefaultId = ZoneId.systemDefault().getId();
        // only IANA region names are accepted; skip when the JVM default is not one
        Assume.assumeTrue(systemDefaultId.contains("/"));
        assertNull(ConnectionParam.resolveTimezone(systemDefaultId));
    }

    @Test
    public void testNullOrEmptyMeansNoTimezone() throws SQLException {
        assertNull(ConnectionParam.resolveTimezone(null));
        assertNull(ConnectionParam.resolveTimezone(""));
    }

    @Test
    public void testOffsetFormsRejected() {
        assertInvalidTimezone("+08:00");
        assertInvalidTimezone("UTC+8");
        assertInvalidTimezone("GMT-8");
        // not an IANA region name
        assertInvalidTimezone("UTC");
    }

    @Test
    public void testInvalidIanaNameRejected() {
        assertInvalidTimezone("Invalid/Zone");
    }

    private static void assertInvalidTimezone(String tz) {
        try {
            ConnectionParam.resolveTimezone(tz);
            fail("expected SQLException for timezone: " + tz);
        } catch (SQLException expected) {
            // expected
        }
    }
}
