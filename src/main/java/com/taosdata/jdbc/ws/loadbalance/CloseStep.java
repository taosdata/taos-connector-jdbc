package com.taosdata.jdbc.ws.loadbalance;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

class CloseStep implements Step {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CloseStep.class);

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // connection is closed
        if (context.getWsClient().isOpen()) {
            context.getWsClient().closeAsync();
        }
        return CompletableFuture.completedFuture(new StepResponse(StepEnum.FINISH, 0));
    }
}