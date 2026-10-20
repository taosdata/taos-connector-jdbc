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

import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class ConCmdStep implements Step {
    private static final Logger log = LoggerFactory.getLogger(ConCmdStep.class);
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // connection is closed
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        // send query
        ConnectReq connectReq = new ConnectReq(context.getParam(),
                context.getParam().isAdapterHa() ? Boolean.FALSE : null);

        Request request = new Request(Action.CONN.getAction(), connectReq);
        String reqString = request.toString();
        context.getWsClient().send(reqString);

        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try{
            context.getInFlightRequest().put(new FutureResponse(request.getAction(), request.id(), completableFuture));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval()));
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, context.getParam().getRequestTimeout(), TimeUnit.MILLISECONDS, reqString);

        return responseFuture
                .thenApply(response -> {
                    context.getInFlightRequest().remove(request.getAction(), request.id());
                    ConnectResp connectResp = (ConnectResp) response;
                    if (Code.SUCCESS.getCode() == connectResp.getCode()) {
                        return new StepResponse(StepEnum.QUERY, 0);
                    } else {
                        context.cleanUp();
                        return new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval());
                    }
                })
                .exceptionally(ex -> {
                    log.info("Connection command failed. {}", ex.getMessage());
                    context.cleanUp();
                    return new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval());
                });
    }
}
