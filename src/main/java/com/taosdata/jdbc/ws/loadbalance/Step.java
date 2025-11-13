package com.taosdata.jdbc.ws.loadbalance;

import java.util.concurrent.CompletableFuture;

interface Step {
    // 执行当前步骤（返回是否需要继续执行下一步）
    CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow);
}