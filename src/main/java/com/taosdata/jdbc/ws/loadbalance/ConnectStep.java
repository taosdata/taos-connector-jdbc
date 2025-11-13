package com.taosdata.jdbc.ws.loadbalance;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

class ConnectStep implements Step {
    static Logger log = org.slf4j.LoggerFactory.getLogger(ConnectStep.class);

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // 若已有活跃连接，直接进入下一步
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
                    context.cleanup(); // 重置组内计数（步骤特有逻辑）
                    return new StepResponse(StepEnum.CONNECT, 0); // 立即返回，等待重试
                });
    }
}