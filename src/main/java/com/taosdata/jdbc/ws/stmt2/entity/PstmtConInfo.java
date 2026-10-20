/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.Transport;

public class PstmtConInfo {
    private final Transport transport;
    private final ConnectionParam param;
    private final String database;
    private final AbstractConnection connection;
    private final Long instanceId;

    public PstmtConInfo(Transport transport, ConnectionParam param, String database, AbstractConnection connection, Long instanceId) {
        this.transport = transport;
        this.param = param;
        this.database = database;
        this.connection = connection;
        this.instanceId = instanceId;
    }

    public Transport getTransport() {
        return transport;
    }

    public ConnectionParam getParam() {
        return param;
    }

    public String getDatabase() {
        return database;
    }

    public AbstractConnection getConnection() {
        return connection;
    }

    public Long getInstanceId() {
        return instanceId;
    }
}
