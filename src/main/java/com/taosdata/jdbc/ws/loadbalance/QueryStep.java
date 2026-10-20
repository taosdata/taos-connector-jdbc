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
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class QueryStep implements Step {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(QueryStep.class);

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        String action = Action.BINARY_QUERY.getAction();
        long reqId = ReqId.getReqID();

        byte[] sqlBytes = "SHOW CLUSTER ALIVE;".getBytes(StandardCharsets.UTF_8);
        int totalLength = 30 + sqlBytes.length;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(0L); // result id
        buffer.writeLongLE(6L);
        buffer.writeShortLE(1);
        buffer.writeIntLE(sqlBytes.length);
        buffer.writeBytes(sqlBytes);


        // send query
        context.getWsClient().send(buffer);

        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try{
            context.getInFlightRequest().put(new FutureResponse(action, reqId, completableFuture));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, 0));
        }

        String reqString = "action:" + action + ", reqId:" + reqId + ", resultId:" + 0 + ", actionType:" + 6;

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, context.getParam().getRequestTimeout(), TimeUnit.MILLISECONDS, reqString);

        return responseFuture
                .thenApply(response -> {
                    context.getInFlightRequest().remove(action, reqId);

                    QueryResp queryResp = (QueryResp) response;
                    if (Code.SUCCESS.getCode() == queryResp.getCode()) {
                        context.setResultId(queryResp.getId());
                        context.addRecoveryCmdCount();
                        return new StepResponse(StepEnum.FETCH, 0);
                    } else {
                        context.cleanUp();
                        return new StepResponse(StepEnum.QUERY, 0);
                    }
                })
                .exceptionally(ex -> {
                    log.info("execute 'show cluster alive' failed. {}", ex.getMessage());
                    context.cleanUp();
                    return new StepResponse(StepEnum.QUERY, context.getRecoveryInterval());
                });
    }
}