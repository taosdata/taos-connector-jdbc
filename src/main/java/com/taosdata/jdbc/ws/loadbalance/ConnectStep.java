package com.taosdata.jdbc.ws.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class ConnectStep implements Step {
    private final Logger log;
    public ConnectStep() {
        this.log = LoggerFactory.getLogger(ConCmdStep.class);
    }
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (context.getWsClient() != null && context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CON_CMD, 0));
        }

        // 调用异步获取Channel的通用方法
        return context.getWsClient().getChannelAsync() // 假设HealthCheckContext中持有getChannelAsync的引用
                .thenApply(channel -> {
                    log.info("Connection established successfully.");
                    return new StepResponse(StepEnum.CON_CMD, 0);
                })
                .exceptionally(ex -> {
                    log.info("Connection or handshake failed.", ex);
                    context.cleanUp();
                    return new StepResponse(StepEnum.CONNECT, context.getNextInterval()); // 立即返回，等待重试
                });
    }
}