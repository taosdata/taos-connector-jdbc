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

package com.taosdata.jdbc.utils;

public class TestEnvUtil {
    private static final String HOST = System.getenv().getOrDefault("TDENGINE_HOST", "localhost");
    private static final int JNI_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_JNI_PORT", "6030"));
    private static final int RS_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_RS_PORT", "6041"));
    private static final int WS_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_WS_PORT", "6041"));
    private static final String USER = System.getenv().getOrDefault("TDENGINE_USER", "root");
    private static final String PASSWORD = System.getenv().getOrDefault("TDENGINE_PASSWORD", "taosdata");

    private TestEnvUtil() {
    }

    public static String getHost() {
        return HOST;
    }
    public static int getJniPort() {
        return JNI_PORT;
    }
    public static int getRsPort() {
        return RS_PORT;
    }
    public static int getWsPort() {
        return WS_PORT;
    }

    public static String getUser() {
        return USER;
    }
    public static String getPassword() {
        return PASSWORD;
    }
}
