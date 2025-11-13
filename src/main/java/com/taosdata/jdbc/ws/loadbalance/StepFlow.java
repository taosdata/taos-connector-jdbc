package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.Utils;
import org.slf4j.Logger;

import java.util.List;

class StepFlow {
    static Logger log = org.slf4j.LoggerFactory.getLogger(StepFlow.class);
    private final List<Step> steps;
    private final BgHealthCheck context;
    private int currentStepIndex; // 当前步骤索引

    public StepFlow(List<Step> steps, BgHealthCheck context) {
        this.steps = steps;
        this.context = context;
    }

    // 启动流程（从第0步开始）
    public void start() {
        currentStepIndex = 0;
        executeCurrentStep();
    }

    // 执行当前步骤
    private void executeCurrentStep() {
        if (currentStepIndex >= steps.size()) {
            return;
        }
        steps.get(currentStepIndex).execute(context, this)
                // 处理步骤返回的结果（无论成功失败，都由控制器决定下一步）
                .whenComplete((stepResponse, ex) -> {
                    if (ex != null) {
                        log.info("Error executing step {}: {}", StepEnum.getNameByInt(currentStepIndex), ex.getMessage());
                    } else {
                        log.info("Step {} returned response: {}", StepEnum.getNameByInt(currentStepIndex), stepResponse);
                    }

                    if (stepResponse.getWaitMs() == 0){
                        currentStepIndex = stepResponse.getStep().get();
                        executeCurrentStep();
                    } else {
                        Utils.getEventLoopGroup().schedule(() -> {
                            currentStepIndex = stepResponse.getStep().get();
                            executeCurrentStep();
                        }, stepResponse.getWaitMs(), java.util.concurrent.TimeUnit.MILLISECONDS);
                    }
                });
    }
}