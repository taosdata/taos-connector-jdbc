package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.common.Cluster;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.EndpointInfo;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.FetchBlockHealthCheckResp;
import com.taosdata.jdbc.ws.loadbalance.BgHealthCheck;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RebalanceUtil {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RebalanceUtil.class);
    private static final AtomicBoolean gRebalancing = new AtomicBoolean(false);
    private static final Map<Cluster, AtomicBoolean> CLUSTER_MAP = new ConcurrentHashMap<>();
    private static final Map<Endpoint, EndpointInfo> ENDPOINT_MAP = new ConcurrentHashMap<>();
    private static final Map<Endpoint, Cluster> ENDPOINT_CLUSTER_MAP = new ConcurrentHashMap<>();
    private RebalanceUtil() {}

    /**
     * Encapsulation class for endpoint connection count statistics
     * Contains: connection count array, minimum value, maximum value, total count
     */
    private static class EndpointCountStats {
        private final int minCount;       // Minimum connection count among endpoints
        private final int minIndex;       // Minimum connection count endpoint's index
        private final int maxCount;       // Maximum connection count among endpoints
        private final int totalCount;     // Total connection count of all endpoints

        public EndpointCountStats(int minCount, int maxCount, int totalCount, int minIndex) {
            this.minCount = minCount;
            this.minIndex = minIndex;
            this.maxCount = maxCount;
            this.totalCount = totalCount;
        }
        public int getMinCount() { return minCount; }
        public int getMinIndex() { return minIndex; }
        public int getMaxCount() { return maxCount; }
        public int getTotalCount() { return totalCount; }
    }

    /**
     * Common method: Collect and calculate endpoint connection count statistics
     * This is the core deduplication logic to avoid repeated traversal of endpoint lists
     * @param endpoints List of endpoints to be counted
     * @return Encapsulated statistics result object
     */
    private static EndpointCountStats collectEndpointCountStats(List<Endpoint> endpoints) {
        int size = endpoints.size();
        int minCount = Integer.MAX_VALUE;
        int maxCount = Integer.MIN_VALUE;
        int minIndex = -1;
        int totalCount = 0;

        // Traverse endpoints once to complete all statistics
        for (int i = 0; i < size; i++) {
            Endpoint endpoint = endpoints.get(i);
            EndpointInfo info = ENDPOINT_MAP.get(endpoint);
            int count = info.getConnectCount();

            totalCount += count;
            if (count < minCount) {
                minCount = count;
                minIndex = i;
            }
            if (count > maxCount) maxCount = count;
        }

        return new EndpointCountStats(minCount, maxCount, totalCount, minIndex);
    }

    public static boolean isRebalancing(Endpoint endpoint) {
        Cluster cluster = ENDPOINT_CLUSTER_MAP.get(endpoint);
        return CLUSTER_MAP.get(cluster).get();
    }

    public static boolean isRebalancing() {
        return gRebalancing.get();
    }

    public static void incrementConnectionCount(Endpoint endpoint) {
         ENDPOINT_MAP.get(endpoint).incrementConnectCount();
    }
    public static void decrementConnectionCount(Endpoint endpoint) {
        ENDPOINT_MAP.get(endpoint).decrementConnectCount();
    }

    public static void connected(List<Endpoint> endpoints, int index) {
        Endpoint endpoint = endpoints.get(index);
        ENDPOINT_MAP.get(endpoint).setOnline();
    }
    public static void disconnected(ConnectionParam original, int index, InFlightRequest inFlightRequest) {
        disconnectedBySelf(original, index);

        if (!StringUtils.isEmpty(original.getCloudToken())) {
            return;
        }
        if (ENDPOINT_MAP.get(original.getEndpoints().get(index)).setOffline() && original.isEnableAutoConnect()) {
            // only once set to offline
            startBackgroundHealthCheck(original, index, inFlightRequest);
        }
    }
    public static void disconnectedBySelf(ConnectionParam original, int index) {
        List<Endpoint> endpoints = original.getEndpoints();
        Endpoint endpoint = endpoints.get(index);

        ENDPOINT_MAP.get(endpoint).decrementConnectCount();
    }

    public static void newCluster(List<Endpoint> endpoints){
        Cluster cluster = new Cluster(endpoints.toArray(new Endpoint[0]));
        if (null == CLUSTER_MAP.putIfAbsent(cluster, new AtomicBoolean(false))) {
            // first time to see this cluster, initialize endpoint info
            for (Endpoint endpoint : endpoints) {
                ENDPOINT_MAP.putIfAbsent(endpoint, new EndpointInfo());
                ENDPOINT_CLUSTER_MAP.putIfAbsent(endpoint, cluster);
            }
        }
    }
    public static void endpointUp(ConnectionParam param, Endpoint endpoint){
        Cluster cluster = ENDPOINT_CLUSTER_MAP.get(endpoint);
        ENDPOINT_MAP.get(endpoint).setOnline();

        if (needRebalancingInner(param)) {
            CLUSTER_MAP.get(cluster).set(true);
            gRebalancing.set(true);
            log.info("endpoint: {} is up, start balancing", endpoint);
        }
    }
    public static EndpointInfo getEndpointInfo(Endpoint endpoint){
        return ENDPOINT_MAP.get(endpoint);
    }

    public static int[] getConnectCountsAsc(List<Endpoint> endpoints) {
        int n = endpoints.size();
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = i;
        }

        for (int i = 1; i < n; i++) {
            int current = indexes[i];
            int currentCount = ENDPOINT_MAP.get(endpoints.get(current)).getConnectCount();
            int j = i - 1;

            while (j >= 0) {
                int jCount = ENDPOINT_MAP.get(endpoints.get(indexes[j])).getConnectCount();
                if (jCount <= currentCount) break;

                indexes[j + 1] = indexes[j];
                j--;
            }
            indexes[j + 1] = current;
        }

        return indexes;
    }

    public static void startBackgroundHealthCheck(ConnectionParam original, int index, InFlightRequest inFlightRequest) {
        ConnectionParam param = ConnectionParam.copyToBuilder(original)
                .build();
        param.setBinaryMessageHandler(byteBuf -> {
            byteBuf.readerIndex(26);
            long id = byteBuf.readLongLE();
            byteBuf.readerIndex(8);
            Utils.retainByteBuf(byteBuf);

            try {
                FetchBlockHealthCheckResp resp = new FetchBlockHealthCheckResp(byteBuf);
                FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
                if (null != remove) {
                    remove.getFuture().complete(resp);
                }
            } catch (Exception e) {
                Utils.releaseByteBuf(byteBuf);
                log.error("Unexpected error handling fetch block data, id: {}", id, e);
                FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
                if (null != remove) {
                    remove.getFuture().complete(FetchBlockHealthCheckResp.getFailedResp());
                }
            }
        });

        // start background health check
        BgHealthCheck bgHealthCheck = new BgHealthCheck(WSFunction.WS, param, index, inFlightRequest);
        bgHealthCheck.start();
    }

    /**
     * Internal method: Check if rebalancing is needed based on connection count distribution
     * @param param Connection parameters (contains rebalance thresholds)
     * @return true if rebalancing is needed, false otherwise
     */
    private static boolean needRebalancingInner(ConnectionParam param) {
        List<Endpoint> endpoints = param.getEndpoints();
        EndpointCountStats stats = collectEndpointCountStats(endpoints);

        // Rebalancing conditions:
        // 1. Total connections exceed the base count threshold
        // 2. Max connection count exceeds min count by the specified percentage
        if (stats.getTotalCount() <= param.getRebalanceConBaseCount()
                || stats.getMaxCount() <= (stats.getMinCount() * (1 + param.getRebalanceThreshold() / 100.0))) {
            return false;
        }
        return true;
    }

    /**
     * Handle rebalancing logic for the current connection
     * @param param Connection parameters
     * @param currentEndpoint The endpoint currently connected
     * @return true if rebalancing is required for this connection, false otherwise
     */
    public static boolean handleRebalancing(ConnectionParam param, Endpoint currentEndpoint) {
        if (!needRebalancingInner(param)) {
            Cluster cluster = ENDPOINT_CLUSTER_MAP.get(currentEndpoint);
            CLUSTER_MAP.get(cluster).set(false);

            // Update global rebalancing state if no cluster is rebalancing
            boolean haveClusterRebalancing = false;
            for (Map.Entry<Cluster, AtomicBoolean> entry : CLUSTER_MAP.entrySet()) {
                if (entry.getValue().get()) {
                    haveClusterRebalancing = true;
                    break;
                }
            }

            if (!haveClusterRebalancing) {
                gRebalancing.set(false);
            }
        }

        int minIndex = getMinConnectionEndpointIndex(param);
        int minCount = ENDPOINT_MAP.get(param.getEndpoints().get(minIndex)).getConnectCount();
        int currentCount = ENDPOINT_MAP.get(currentEndpoint).getConnectCount();
        // No rebalancing needed if current count is close to min count (within threshold)
        if (currentCount == minCount || currentCount <= (minCount * (1 + param.getRebalanceThreshold() / 100.0))) {
            return false;
        }

        return true;
    }

    /**
     * Get the index of the endpoint with the minimum connection count
     * @param param Connection parameters
     * @return Index of the endpoint with the least connections (-1 if empty)
     */
    public static int getMinConnectionEndpointIndex(ConnectionParam param) {
        List<Endpoint> endpoints = param.getEndpoints();
        EndpointCountStats stats = collectEndpointCountStats(endpoints);
        return stats.minIndex;
    }
}
