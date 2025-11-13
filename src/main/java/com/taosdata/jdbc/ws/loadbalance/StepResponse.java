package com.taosdata.jdbc.ws.loadbalance;

public class StepResponse {
    private StepEnum step;
    private int waitMs;

    public StepResponse(StepEnum step, int waitMs) {
        this.step = step;
        this.waitMs = waitMs;
    }

    public StepEnum getStep() {
        return step;
    }
    public int getWaitMs() {
        return waitMs;
    }

    public String toString() {
        return "StepResponse{stepIndex=" + StepEnum.getNameByInt(step.get()) + ", waitMs=" + waitMs + "}";
    }
}
