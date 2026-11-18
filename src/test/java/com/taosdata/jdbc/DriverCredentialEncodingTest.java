/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulConnection;
import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.WebSocketDriver;
import com.taosdata.jdbc.ws.entity.ConnectReq;
import com.taosdata.jdbc.ws.tmq.entity.ConsumerParam;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Properties;

import static org.junit.Assert.*;

public class DriverCredentialEncodingTest {
    @Test
    public void propertiesPreserveLiteralCredentials() throws SQLException {
        String user = "user+%2B";
        for (String password : new String[]{"a+b\\c", "%2B", "%25", "100%", "%ZZ", "密 码&="}) {
            assertAuthentication("", credentials(user, password), user, password);
        }
    }

    @Test
    public void urlCredentialsAreDecodedOnceAndOverrideProperties() throws SQLException {
        assertAuthentication("?user=u%2Bser&password=%E5%AF%86+%2B%5C%252B%2525%25%26%3D",
                credentials("other", "other"), "u+ser", "密 +\\%2B%25%&=");
    }

    @Test
    public void mixedSourcesAreHandledIndependently() throws SQLException {
        assertAuthentication("?user=u%2Bser", credentials("other", "a+%2B"), "u+ser", "a+%2B");
        assertAuthentication("?password=p%2B%25", credentials("u+%2B", "other"), "u+%2B", "p+%");
    }

    @Test
    public void malformedUrlCredentialReportsTheParameterWithoutItsValue() {
        for (String key : new String[]{"user", "password"}) {
            String query = "localhost/?" + key + "=private%ZZ";
            for (AbstractDriver driver : new AbstractDriver[]{new RestfulDriver(), new WebSocketDriver()}) {
                String prefix = driver instanceof RestfulDriver ? RestfulDriver.URL_PREFIX : WebSocketDriver.URL_PREFIX;
                SQLException error = assertThrows(SQLException.class,
                        () -> driver.connect(prefix + query, credentials("user", "password")));
                assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, error.getErrorCode());
                assertTrue(error.getMessage().contains(key));
                assertFalse(error.getMessage().contains("private"));
            }
        }
    }

    @Test
    public void tokenAuthenticationPreservesOpaqueValues() throws SQLException {
        String token = "t+%2B%";
        String url = WebSocketDriver.URL_PREFIX + "localhost/?user=unused&password=%ZZ";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BEARER_TOKEN, token);
        ConnectionParam bearer = ConnectionParam.getParamWs(StringUtils.parseUrl(url, properties));
        assertEquals(token, bearer.getBearerToken());
        ConnectionParam cloud = ConnectionParam.getParam(StringUtils.parseUrl(url + "&token=" + token, null));
        assertEquals(token, cloud.getCloudToken());
    }

    @Test
    public void nativeDriverRetainsLiteralUrlCredentials() throws SQLException {
        Properties parsed = new TSDBDriver().parseURL("jdbc:TAOS://localhost/?user=u+ser&password=p%2B%", null);
        assertEquals("u+ser", parsed.getProperty("user"));
        assertEquals("p%2B%", parsed.getProperty("password"));
    }

    @Test
    public void consumerUsesTheSourceOfTheEffectiveTmqCredential() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "overridden");
        properties.setProperty(TMQConstants.CONNECT_PASS, "p+%2B%");
        properties.setProperty(TMQConstants.CONNECT_URL, WebSocketDriver.URL_PREFIX
                + "localhost/?user=other&password=ignored%ZZ&td.connect.user=u%2B%252B");
        ConnectionParam param = new ConsumerParam(properties).getConnectionParam();
        assertEquals("u+%2B", param.getUser());
        assertEquals("p+%2B%", param.getPassword());
    }

    @Test
    public void reusingConsumerPropertiesPreservesTheOriginalTmqPassword() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "user");
        properties.setProperty(TMQConstants.CONNECT_PASS, "p+%2B");
        String url = WebSocketDriver.URL_PREFIX + "localhost:6041/";
        properties.setProperty(TMQConstants.CONNECT_URL, url + "?td.connect.pass=p%2B%252B");
        assertEquals("p+%2B", new ConsumerParam(properties).getConnectionParam().getPassword());

        properties.setProperty(TMQConstants.CONNECT_URL, url);
        assertEquals("p+%2B", new ConsumerParam(properties).getConnectionParam().getPassword());
    }

    private static Properties credentials(String user, String password) {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return properties;
    }

    private static void assertAuthentication(String query, Properties properties, String user, String password)
            throws SQLException {
        String address = "localhost:6041/" + query;
        try (RestfulConnection rest = (RestfulConnection) new RestfulDriver().connect(
                RestfulDriver.URL_PREFIX + address, (Properties) properties.clone())) {
            String expected = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
            assertEquals("Basic " + expected, rest.getAuth());
        }

        CapturingWebSocketDriver ws = new CapturingWebSocketDriver();
        ws.connect(WebSocketDriver.URL_PREFIX + address, properties);
        assertEquals(user, ws.request.getUser());
        assertEquals(password, ws.request.getPassword());
    }

    private static class CapturingWebSocketDriver extends WebSocketDriver {
        private ConnectReq request;

        @Override
        protected Connection getWSConnection(String url, ConnectionParam param, Properties props) {
            request = new ConnectReq(param);
            return null;
        }
    }
}
