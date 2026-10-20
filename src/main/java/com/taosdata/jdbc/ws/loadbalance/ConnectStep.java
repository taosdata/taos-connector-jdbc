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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class ConnectStep implements Step {
    private static final Logger log = LoggerFactory.getLogger(ConnectStep.class);
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (context.isCancelled()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.FINISH, 0));
        }

        if (context.getWsClient() != null && context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CON_CMD, 0));
        }

        // call the common method to get Channel asynchronously
        return context.getWsClient().getChannelAsync()
                .thenApply(channel -> {
                    log.info("Connection established successfully.");
                    return new StepResponse(StepEnum.CON_CMD, 0);
                })
                .exceptionally(ex -> {
                    log.info("Connection or handshake failed. endpoint: {}", context.getEndpoint());
                    context.cleanUp();
                    return new StepResponse(StepEnum.CONNECT, context.getNextInterval());
                });
    }
}