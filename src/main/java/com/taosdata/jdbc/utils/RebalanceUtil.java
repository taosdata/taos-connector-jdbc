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
    private static final AtomicBoolean g_reblancing = new AtomicBoolean(false);
    private static final Map<Cluster, AtomicBoolean> CLUSTER_MAP = new ConcurrentHashMap<>();
    private static final Map<Endpoint, EndpointInfo> ENDPOINT_MAP = new ConcurrentHashMap<>();
    private static final Map<Endpoint, Cluster> ENDPOINT_CLUSTER_MAP = new ConcurrentHashMap<>();
    private RebalanceUtil() {}
    public static boolean isReblancing(Cluster cluster) {
        return CLUSTER_MAP.get(cluster).get();
    }

    public static boolean isReblancing() {
        return g_reblancing.get();
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

        if (ENDPOINT_MAP.get(original.getEndpoints().get(index)).setOffline()) {
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
    public static synchronized void endpointUp(Endpoint endpoint){
        Cluster cluster = ENDPOINT_CLUSTER_MAP.get(endpoint);
        CLUSTER_MAP.get(cluster).set(true);
        g_reblancing.set(true);

        log.info("endpoint: " + endpoint + " is up, start rebalancing");
    }
    private static synchronized void rebalanceDone(Endpoint endpoint){
        Cluster cluster = ENDPOINT_CLUSTER_MAP.get(endpoint);
        CLUSTER_MAP.get(cluster).set(false);

        if (CLUSTER_MAP.values().stream().noneMatch(AtomicBoolean::get)){
            g_reblancing.set(false);
        }
    }

    public static EndpointInfo getEndpointInfo(Endpoint endpoint){
        return ENDPOINT_MAP.get(endpoint);
    }

    public static int[] getConnectCountsAsc(List <Endpoint> endpoints){
        int n = endpoints.size();
        // Step 1: Initialize index array (indexes[i] = i)
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = i;
        }

        // Step 2: Selection sort based on connect counts
        for (int i = 0; i < n - 1; i++) {
            int minIdx = i;
            for (int j = i + 1; j < n; j++) {
                if (ENDPOINT_MAP.get(endpoints.get(indexes[j])).getConnectCount()
                        < ENDPOINT_MAP.get(endpoints.get(indexes[minIdx])).getConnectCount()) {
                    minIdx = j;
                }
            }
            // exchange
            int temp = indexes[minIdx];
            indexes[minIdx] = indexes[i];
            indexes[i] = temp;
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
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(byteBuf.readerIndex(), bytes);

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

}
