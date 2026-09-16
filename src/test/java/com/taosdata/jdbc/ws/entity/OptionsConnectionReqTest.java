package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.taosdata.jdbc.utils.JsonUtil;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for OptionsConnectionReq (no mock frameworks, pure real-instance testing)
 */
public class OptionsConnectionReqTest {

    @Test
    public void testTimezoneRequestSerialization() throws Exception {
        Request request = new Request(Action.OPTIONS_CONNECTION.getAction(),
                OptionsConnectionReq.ofTimezone("Asia/Shanghai"));

        JsonNode root = JsonUtil.getObjectMapper().readTree(request.toString());
        assertEquals("options_connection", root.get("action").asText());

        JsonNode args = root.get("args");
        assertTrue(args.has("req_id"));

        JsonNode options = args.get("options");
        assertTrue(options.isArray());
        assertEquals(1, options.size());

        JsonNode option = options.get(0);
        assertEquals(OptionsConnectionReq.OPTION_CONNECTION_TIMEZONE, option.get("option").asInt());
        assertEquals("Asia/Shanghai", option.get("value").asText());
    }

    @Test
    public void testNullValueClearsOption() throws Exception {
        Request request = new Request(Action.OPTIONS_CONNECTION.getAction(),
                OptionsConnectionReq.ofTimezone(null));

        JsonNode option = JsonUtil.getObjectMapper().readTree(request.toString())
                .get("args").get("options").get(0);
        assertEquals(OptionsConnectionReq.OPTION_CONNECTION_TIMEZONE, option.get("option").asInt());
        // a null value (or an omitted value field) clears the option on the server side
        assertTrue(!option.has("value") || option.get("value").isNull());
    }

    @Test
    public void testOptionGettersSetters() {
        OptionsConnectionReq.Option option = new OptionsConnectionReq.Option();
        option.setOption(OptionsConnectionReq.OPTION_CONNECTION_TIMEZONE);
        option.setValue("Asia/Tokyo");
        assertEquals(OptionsConnectionReq.OPTION_CONNECTION_TIMEZONE, option.getOption());
        assertEquals("Asia/Tokyo", option.getValue());

        OptionsConnectionReq req = new OptionsConnectionReq();
        assertTrue(req.getReqId() != 0);
    }
}
