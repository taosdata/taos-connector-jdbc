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

package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.ws.FetchBlockData;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.QueryResp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class MockTransport extends Transport {
    private FetchBlockData target;
    private int sendFetchCount = 0;

    public static final long TEST_QUERY_ID = 12345;
    public static final long TEST_REQ_ID = 67890;
    public static final int TIMEOUT_MS = 100;
    public static final int PRECISION = 0;
    public static final List<RestfulResultSet.Field> TEST_FIELDS = Arrays.asList(
            new RestfulResultSet.Field("ts", 0, 8, "timestamp", TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, 0, 0),
            new RestfulResultSet.Field("name", 0, 4, "name", TSDBConstants.TSDB_DATA_TYPE_INT, 0, 0)
    );

    public MockTransport() throws SQLException {
        super();
    }

    public void setTarget(FetchBlockData target) {
        this.target = target;
    }

    @Override
    public void sendFetchBlockAsync(long reqId, long queryId) {
        sendFetchCount++;
    }

    public void simulateReceiveResponse(FetchBlockNewResp resp) throws InterruptedException {
        target.handleReceiveBlockData(resp);
    }

    public int getSendFetchCount() {
        return sendFetchCount;
    }

    public void clearSendFetchCount() {
        sendFetchCount = 0;
    }

    public static QueryResp createQueryResp() {
        QueryResp resp = new QueryResp();
        resp.setId(TEST_QUERY_ID);
        resp.setReqId(TEST_REQ_ID);
        return resp;
    }
}