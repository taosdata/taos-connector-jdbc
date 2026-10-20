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

package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class RebalanceTestUtil {

    private static final Logger log = LoggerFactory.getLogger(RebalanceTestUtil.class);
    public static void waitHealthCheckFinished(Endpoint endpoint) throws InterruptedException, SQLException {
        if (RebalanceManager.getInstance().getBgHealthCheckInstanceCount() == 0) {
            TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Health check thread is not running");
        }

        int maxWaitMs = 3000;
        while (!RebalanceManager.getInstance().isRebalancing(endpoint) && maxWaitMs > 0) {
            Thread.sleep(100);
            maxWaitMs -= 100;
        }
        // wait a bit more to ensure health check is done
        Thread.sleep(100);
        if (!RebalanceManager.getInstance().isRebalancing(endpoint)) {
            log.error("Health check did not complete in expected time for endpoint: {}", endpoint);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Health check did not complete in expected time");
        }
    }

    public static void waitHealthCheckFinishedIgnoreException(Endpoint endpoint) {
        try {
            waitHealthCheckFinished(endpoint);
        } catch (Exception e) {
            // Ignore exceptions
        }
    }
}
