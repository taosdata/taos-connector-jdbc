/*
 * Copyright (c) 2022 TAOS Data, Inc.
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

package com.taosdata.jdbc.utils;

public class SpecifyAddress {
    private static final int JNI_PORT_DEFAULT = 6030;
    private static final int REST_PORT_DEFAULT = 6041;
    private static final int WEB_SOCKET_PORT_DEFAULT = 6041;
    private String jniUrl = null;
    private String jniWithoutPropUrl = null;
    private String host = null;
    private String jniPort = null;
    private String restUrl = null;
    private String restWithoutPropUrl = null;
    private String restPort = null;

    private String webSocketUrl = null;
    private String webSocketWithoutPropUrl = null;
    private String webSocketPort = null;
    private String userAndPassword = "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
    private SpecifyAddress() {
        String host = System.getProperty("maven.test.host");
        if (null != host && !"".equals(host.trim())) {
            this.host = host.trim();
            String jni = System.getProperty("maven.test.port.jni");
            if (null != jni && !"".equals(jni.trim())) {
                jniUrl = "jdbc:TAOS://" + host.trim() + ":" + jni.trim() + "/?" + userAndPassword;
                jniWithoutPropUrl = "jdbc:TAOS://" + host.trim() + ":" + jni.trim() + "/";

                jniPort = jni.trim();
            } else {
                jniUrl = "jdbc:TAOS://" + host.trim() + ":" + JNI_PORT_DEFAULT + "/?" + userAndPassword;
                jniWithoutPropUrl = "jdbc:TAOS://" + host.trim() + ":" + JNI_PORT_DEFAULT + "/";
                jniPort = JNI_PORT_DEFAULT + "";
            }
            String rest = System.getProperty("maven.test.port.rest");
            if (null != rest && !"".equals(rest.trim())) {
                restUrl = "jdbc:TAOS-RS://" + host.trim() + ":" + rest.trim() + "/?" + userAndPassword;
                restWithoutPropUrl = "jdbc:TAOS-RS://" + host.trim() + ":" + rest.trim() + "/";
                restPort = rest.trim();
            } else {
                restUrl = "jdbc:TAOS-RS://" + host.trim() + ":" + REST_PORT_DEFAULT + "/?" + userAndPassword;
                restWithoutPropUrl = "jdbc:TAOS-RS://" + host.trim() + ":" + REST_PORT_DEFAULT + "/";
                restPort = REST_PORT_DEFAULT + "";
            }

            String websocket = System.getProperty("maven.test.port.websocket");
            if (null != websocket && !"".equals(websocket.trim())) {
                webSocketUrl = "jdbc:TAOS-WS://" + host.trim() + ":" + rest.trim() + "/?" + userAndPassword;
                webSocketWithoutPropUrl = "jdbc:TAOS-WS://" + host.trim() + ":" + rest.trim() + "/";
                webSocketPort = websocket.trim();
            } else {
                webSocketUrl = "jdbc:TAOS-WS://" + host.trim() + ":" + WEB_SOCKET_PORT_DEFAULT + "/?" + userAndPassword;
                webSocketWithoutPropUrl = "jdbc:TAOS-WS://" + host.trim() + ":" + WEB_SOCKET_PORT_DEFAULT + "/";
                webSocketPort = WEB_SOCKET_PORT_DEFAULT + "";
            }
        }
    }

    private static final SpecifyAddress instance = new SpecifyAddress();

    public static SpecifyAddress getInstance() {
        return instance;
    }

    public String getJniUrl() {
        return jniUrl;
    }

    public String getJniWithoutUrl() {
        return jniWithoutPropUrl;
    }

    public String getJniPort() {
        return jniPort;
    }

    public String getRestUrl() {
        return restUrl;
    }

    public String getRestWithoutUrl() {
        return restWithoutPropUrl;
    }

    public String getRestPort() {
        return restPort;
    }

    public String getHost() {
        return host;
    }

    public String getWebSocketUrl() {
        return webSocketUrl;
    }

    public String getWebSocketWithoutUrl() {
        return webSocketWithoutPropUrl;
    }

    public String getWebSocketPort() {
        return webSocketPort;
    }
}

