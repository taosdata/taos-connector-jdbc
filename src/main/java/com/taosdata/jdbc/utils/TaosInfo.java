/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

public class TaosInfo implements TaosInfoMBean {
    private final AtomicLong connectOpen = new AtomicLong();
    private final AtomicLong connectClose = new AtomicLong();
    private final AtomicLong statementCount = new AtomicLong();

    static {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("TaosInfoMBean:name=TaosInfo");
            server.registerMBean(TaosInfo.getInstance(), name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException ignored) {
            throw new RuntimeException("registerMBean failed");
        }
    }

    @Override
    public long getConnectOpen() {
        return connectOpen.get();
    }

    @Override
    public long getConnectClose() {
        return connectClose.get();
    }

    @Override
    public long getConnect_active() {
        return connectOpen.get() - connectClose.get();
    }

    @Override
    public long getStatementCount() {
        return statementCount.get();
    }

    /*******************************************************/

    public void connOpenIncrement() {
        connectOpen.incrementAndGet();
    }

    public void connectCloseIncrement() {
        connectClose.incrementAndGet();
    }

    public void stmtCountIncrement() {
        statementCount.incrementAndGet();
    }

    /********************************************************************************/
    private TaosInfo() {
    }

    private static class TaosInfoHolder {
        private static final TaosInfo INSTANCE = new TaosInfo();
    }

    public static TaosInfo getInstance() {
        return TaosInfoHolder.INSTANCE;
    }

}
