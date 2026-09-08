package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionUtilQueryNsTest {

    @Test
    public void supportQueryNs_acceptsMinimumVersion() {
        assertTrue(VersionUtil.supportQueryNs(TSDBConstants.MIN_QUERY_NS_VERSION));
    }

    @Test
    public void supportQueryNs_acceptsVersionsAboveMinimum() {
        assertTrue(VersionUtil.supportQueryNs("3.4.1.12"));
        assertTrue(VersionUtil.supportQueryNs("3.4.1.14.alpha.community"));
        assertTrue(VersionUtil.supportQueryNs("3.4.2.0"));
        assertTrue(VersionUtil.supportQueryNs("4.0.0.0"));
    }

    @Test
    public void supportQueryNs_rejectsVersionsBelowMinimum() {
        assertFalse(VersionUtil.supportQueryNs("3.4.1.10"));
        assertFalse(VersionUtil.supportQueryNs("3.4.0.99"));
        assertFalse(VersionUtil.supportQueryNs("3.3.6.0"));
    }

    @Test
    public void supportQueryNs_rejectsUnknownOrInvalidVersions() {
        assertFalse(VersionUtil.supportQueryNs(null));
        assertFalse(VersionUtil.supportQueryNs(""));
        assertFalse(VersionUtil.supportQueryNs(TSDBConstants.UNKNOWN_VERSION));
        assertFalse(VersionUtil.supportQueryNs("3.x.y.z"));
    }
}
