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
