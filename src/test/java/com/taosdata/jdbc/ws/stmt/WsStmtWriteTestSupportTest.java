package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WsStmtWriteTestSupportTest {

    @Test
    public void traditionalWebSocketUrl_appendsCredentialsAndModeToOverrideUrl() {
        String url = WsStmtWriteTestSupport.webSocketUrl(
                "jdbc:TAOS-WS://example.com:6041/",
                true,
                false);

        assertEquals("jdbc:TAOS-WS://example.com:6041/?user=" + TestEnvUtil.getUser()
                + "&password=" + TestEnvUtil.getPassword()
                + "&stmtBindMode=traditional", url);
    }

    @Test
    public void traditionalWebSocketUrl_usesWebSocketDefaultWhenNoOverrideExists() {
        String url = WsStmtWriteTestSupport.webSocketUrl(null, true, false);

        assertTrue(url.startsWith("jdbc:TAOS-WS://" + TestEnvUtil.getHost()
                + ":" + TestEnvUtil.getWsPort() + "/?"));
        assertTrue(url.contains("user=" + TestEnvUtil.getUser()));
        assertTrue(url.contains("password=" + TestEnvUtil.getPassword()));
        assertTrue(url.contains("stmtBindMode=traditional"));
    }
}
