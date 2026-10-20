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

import com.taosdata.jdbc.utils.Utils;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

class StepFlow {
    static final Logger log = org.slf4j.LoggerFactory.getLogger(StepFlow.class);
    private final List<Step> steps;
    private final BgHealthCheck context;
    private int currentStepIndex;

    public StepFlow(List<Step> steps, BgHealthCheck context) {
        this.steps = steps;
        this.context = context;
    }

    public void start() {
        currentStepIndex = 0;
        executeCurrentStep();
    }

    private void executeCurrentStep() {
        if (currentStepIndex >= steps.size()) {
            RebalanceManager.getInstance().removeBgHealthCheckInstance();
            RebalanceManager.getInstance().removeBgHealthCheck(context);
            return;
        }
        steps.get(currentStepIndex).execute(context, this)
                // handles the result of the step (regardless of success or failure, it is up to the controller to decide the next step)
                .whenComplete((stepResponse, ex) -> {
                    if (ex != null) {
                        log.info("Error executing step {}: {}", StepEnum.getNameByInt(currentStepIndex), ex.getMessage());
                    } else {
                        log.info("Step {} returned response: {}", StepEnum.getNameByInt(currentStepIndex), stepResponse);
                    }

                    if (stepResponse.getWaitSeconds() == 0){
                        currentStepIndex = stepResponse.getStep().get();
                        executeCurrentStep();
                    } else {
                        Utils.getEventLoopGroup().schedule(() -> {
                            currentStepIndex = stepResponse.getStep().get();
                            executeCurrentStep();
                        }, stepResponse.getWaitSeconds(), TimeUnit.SECONDS);
                    }
                });
    }
}