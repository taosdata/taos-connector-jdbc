package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.WSClient;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

// Health check node context: manages node state, health check parameters, and timer tasks
public class BgHealthCheck {
    static Logger log = org.slf4j.LoggerFactory.getLogger(BgHealthCheck.class);
    private WSClient wsClient; // Reference to the WebSocket client for connection handling
    private ConnectionParam param;
    private int endpointIndex;
    private StepFlow flow;

    private int sleepMinMs = 1000;
    private int sleepMaxMs = 5000;
    private int currentInterval = sleepMinMs;
    private final InFlightRequest inFlightRequest;

    public long getResultId() {
        return resultId;
    }

    public void setResultId(long resultId) {
        this.resultId = resultId;
    }

    private long resultId = -1;

    public BgHealthCheck(WSFunction wsFunction, ConnectionParam param, int index, InFlightRequest inFlightRequest) {
        endpointIndex = index;
        this.param = param;
        this.inFlightRequest = inFlightRequest;
        try {
            wsClient = WSClient.getInstance(param, index, wsFunction);
        } catch (Exception e) {
            log.error("Failed to initialize WSClient for health check: {}", e.getMessage());
            return;
        }

        // 初始化步骤列表（按执行顺序）
        List<Step> steps = Arrays.asList(
                new ConnectStep(),
                new ConCmdStep()
//                new QueryStep(),
//                new FetchStep(),
//                new Fetch2Step()
        );
        this.flow = new StepFlow(steps, this);
        this.flow.start();
    }

    public void cleanup() {

    }

    public WSClient getWsClient() {
        return wsClient;
    }

    public ConnectionParam getParam() {
        return param;
    }

    public int getEndpointIndex() {
        return endpointIndex;
    }

    public InFlightRequest getInFlightRequest() {
        return inFlightRequest;
    }

    public int getCurrentInterval() {
        return currentInterval;
    }
}