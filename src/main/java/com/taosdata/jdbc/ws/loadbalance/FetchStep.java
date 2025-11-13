package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.FetchDataUtil;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.FetchBlockData;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.QueryResp;
import com.taosdata.jdbc.ws.entity.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class FetchStep implements Step {
    static Logger log = org.slf4j.LoggerFactory.getLogger(FetchStep.class);

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // 若已有活跃连接，直接进入下一步
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, 0)); // 立即返回，等待连接建立
        }

        final byte[] version = {1, 0};

        int totalLength = 26;
        long reqId = ReqId.getReqID();
        String action = Action.FETCH_BLOCK.getAction();
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(context.getResultId());
        buffer.writeLongLE(7); // fetch block action type
        buffer.writeBytes(version);

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


        fetchBlockData = new FetchBlockData(transport, response, fields, transport.getConnectionParam().getRequestTimeout(), timestampPrecision);
        FetchDataUtil.getFetchMap().put(reqId, fetchBlockData);

        return responseFuture
                // 处理成功的结果（当 Future 正常完成时执行）
                .thenApply(response -> {
                    context.getInFlightRequest().remove(action, reqId);

                    QueryResp queryResp = (QueryResp) response;
                    if (Code.SUCCESS.getCode() == queryResp.getCode()) {
                        return new StepResponse(StepEnum.FINISH, 0);
                    } else {
                        return new StepResponse(StepEnum.CONNECT, 0);
                    }
                })
                // 处理异常（包括超时、业务异常等，当 Future 异常完成时执行）
                .exceptionally(ex -> {

                    log.info("Connection command failed.", ex);
                    return new StepResponse(StepEnum.CONNECT, context.getCurrentInterval());
                });
    }
}