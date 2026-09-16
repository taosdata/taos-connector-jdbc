package com.taosdata.jdbc.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.CommonResp;
import com.taosdata.jdbc.ws.entity.ConnectReq;
import com.taosdata.jdbc.ws.entity.Request;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link WSConnection#setTimezone(String)} / {@link WSConnection#getTimezone()}.
 * Transport is stubbed with Mockito; no live server is required.
 */
public class WSConnectionSetTimezoneTest {

    private Transport transport;
    private ConnectionParam param;
    private WSConnection connection;

    @Before
    public void setUp() throws SQLException {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        connection = new WSConnection("jdbc:TAOS-WS://localhost:6041/testdb",
                new Properties(), transport, param, "3.4.0.0");
    }

    private void stubSendResponse(int code, String message) throws SQLException {
        CommonResp resp = new CommonResp();
        resp.setCode(code);
        resp.setMessage(message);
        Mockito.when(transport.send(any(Request.class), anyLong())).thenReturn(resp);
    }

    private JsonNode captureSentOption() throws Exception {
        ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
        verify(transport, times(1)).send(captor.capture(), anyLong());
        JsonNode root = JsonUtil.getObjectMapper().readTree(captor.getValue().toString());
        assertEquals(Action.OPTIONS_CONNECTION.getAction(), root.get("action").asText());
        return root.get("args").get("options").get(0);
    }

    @Test
    public void testSetTimezoneSuccess() throws Exception {
        stubSendResponse(0, "");

        connection.setTimezone("Asia/Tokyo");

        JsonNode option = captureSentOption();
        assertEquals(1, option.get("option").asInt());
        assertEquals("Asia/Tokyo", option.get("value").asText());

        verify(param).setTz("Asia/Tokyo");
        verify(param).setZoneId(ConnectionParam.resolveTimezone("Asia/Tokyo"));
    }

    @Test
    public void testSetTimezoneServerError() throws Exception {
        stubSendResponse(0x0B, "set connection timezone error");

        try {
            connection.setTimezone("Asia/Tokyo");
            fail("expected SQLException");
        } catch (SQLException expected) {
            // expected
        }

        // local state must not change when the server rejects the option
        verify(param, never()).setTz(anyString());
        verify(param, never()).setZoneId(any());
    }

    @Test
    public void testSetTimezoneInvalidValue() throws Exception {
        try {
            connection.setTimezone("+08:00");
            fail("expected SQLException");
        } catch (SQLException expected) {
            // expected
        }

        // nothing is sent for a locally rejected timezone
        verify(transport, never()).send(any(Request.class), anyLong());
        verify(param, never()).setTz(anyString());
    }

    @Test
    public void testClearTimezone() throws Exception {
        stubSendResponse(0, "");

        connection.setTimezone(null);

        JsonNode option = captureSentOption();
        assertEquals(1, option.get("option").asInt());
        assertTrue(!option.has("value") || option.get("value").isNull());

        verify(param).setTz("");
        verify(param).setZoneId(null);
    }

    @Test
    public void testGetTimezone() throws SQLException {
        Mockito.when(param.getTz()).thenReturn("Asia/Tokyo");
        assertEquals("Asia/Tokyo", connection.getTimezone());

        Mockito.when(param.getTz()).thenReturn("");
        assertNull(connection.getTimezone());

        Mockito.when(param.getTz()).thenReturn(null);
        assertNull(connection.getTimezone());
    }

    @Test
    public void testReconnectConnMessageCarriesNewTimezone() throws Exception {
        // Reconnect re-authenticates with new ConnectReq(connectionParam), which reads the
        // shared ConnectionParam instance; a timezone set on the open connection must be
        // reflected in the next reconnect handshake.
        ConnectionParam realParam = new ConnectionParam.Builder(new ArrayList<>())
                .setUserAndPassword("root", "taosdata")
                .build();
        WSConnection conn = new WSConnection("jdbc:TAOS-WS://localhost:6041/testdb",
                new Properties(), transport, realParam, "3.4.0.0");
        stubSendResponse(0, "");

        conn.setTimezone("Asia/Tokyo");

        ConnectReq reconnectReq = new ConnectReq(realParam, null);
        assertEquals("Asia/Tokyo", reconnectReq.getTz());
    }
}
