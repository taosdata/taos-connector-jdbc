package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.EndpointInfo;
import com.taosdata.jdbc.rs.ConnectionParam;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for RebalanceUtil, covering core logic including cluster initialization,
 * connection count statistics, rebalancing judgment, and endpoint state management.
 */
public class RebalanceUtilTest {

    private final Endpoint endpoint1 = new Endpoint("endpoint1", 6041, false);
    private final Endpoint endpoint2 = new Endpoint("endpoint2", 6042, false);
    private final Endpoint endpoint3 = new Endpoint("endpoint3", 6043, true);
    @Mock
    private ConnectionParam connectionParam;

    // Test endpoint list
    private List<Endpoint> testEndpoints;

    /**
     * Set up test environment before each test case:
     * 1. Initialize Mock objects via MockitoAnnotations
     * 2. Clear private static maps (via reflection) to avoid cross-test contamination
     * 3. Reset global rebalancing state (via reflection)
     * 4. Initialize test endpoint list and mock ConnectionParam configuration
     */
    @Before
    public void setUp() {
        // 1. Initialize Mock objects (no need for MockitoJUnitRunner)
        MockitoAnnotations.openMocks(this);

        // 2. Clear private static maps via reflection
        clearPrivateStaticMap(RebalanceUtil.class, "CLUSTER_MAP");
        clearPrivateStaticMap(RebalanceUtil.class, "ENDPOINT_MAP");
        clearPrivateStaticMap(RebalanceUtil.class, "ENDPOINT_CLUSTER_MAP");

        // 3. Reset global rebalancing state (gRebalancing) via reflection
        resetPrivateStaticAtomicBoolean(RebalanceUtil.class, "gRebalancing", false);

        // 4. Initialize test endpoint list and mock ConnectionParam
        testEndpoints = new ArrayList<>();
        testEndpoints.add(endpoint1);
        testEndpoints.add(endpoint2);
        testEndpoints.add(endpoint3);

        when(connectionParam.getEndpoints()).thenReturn(testEndpoints);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10); // Rebalance base threshold
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);    // Rebalance threshold (20%)
    }

    /**
     * Get private static field via reflection
     * @param clazz Target class (RebalanceUtil.class)
     * @param fieldName Name of the private static field (e.g., "CLUSTER_MAP")
     * @return Private static field object
     */
    private static Field getPrivateStaticField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true); // Allow access to private field
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Private static field '" + fieldName + "' not found in " + clazz.getName(), e);
        }
    }

    /**
     * Clear private static Map (ConcurrentHashMap) via reflection
     * @param clazz Target class (RebalanceUtil.class)
     * @param fieldName Name of the private static Map field
     */
    private static void clearPrivateStaticMap(Class<?> clazz, String fieldName) {
        try {
            Field field = getPrivateStaticField(clazz, fieldName);
            ConcurrentHashMap<?, ?> map = (ConcurrentHashMap<?, ?>) field.get(null); // static field: param is null
            map.clear();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to clear private static map '" + fieldName + "'", e);
        }
    }

    /**
     * Reset private static AtomicBoolean via reflection
     * @param clazz Target class (RebalanceUtil.class)
     * @param fieldName Name of the private static AtomicBoolean field
     * @param value Target value to set
     */
    private static void resetPrivateStaticAtomicBoolean(Class<?> clazz, String fieldName, boolean value) {
        try {
            Field field = getPrivateStaticField(clazz, fieldName);
            AtomicBoolean atomicBoolean = (AtomicBoolean) field.get(null); // static field: param is null
            atomicBoolean.set(value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to reset private static AtomicBoolean '" + fieldName + "'", e);
        }
    }

    /**
     * Invoke private static method via reflection
     * @param clazz Target class (RebalanceUtil.class)
     * @param methodName Name of the private static method (e.g., "needRebalancingInner")
     * @param paramTypes Parameter types of the method (e.g., ConnectionParam.class)
     * @param params Actual parameters to pass to the method
     * @return Return value of the method
     */
    @SuppressWarnings("unchecked")
    private static <T> T invokePrivateStaticMethod(Class<?> clazz, String methodName, Class<?>[] paramTypes, Object... params) {
        try {
            Method method = clazz.getDeclaredMethod(methodName, paramTypes);
            method.setAccessible(true); // Allow access to private method
            return (T) method.invoke(null, params); // static method: first param is null
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Private static method '" + methodName + "' not found in " + clazz.getName(), e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke private static method '" + methodName + "'", e);
        }
    }

    @Test
    public void newCluster_ShouldInitializeClusterAndEndpoints() {
        // When: Call newCluster to initialize
        RebalanceUtil.newCluster(testEndpoints);

        // Then: Verify via public methods (indirectly confirm initialization)
        assertNotNull(RebalanceUtil.getEndpointInfo(endpoint1));
        assertNotNull(RebalanceUtil.getEndpointInfo(endpoint2));
        assertNotNull(RebalanceUtil.getEndpointInfo(endpoint3));

        EndpointInfo info1 = RebalanceUtil.getEndpointInfo(endpoint1);
        assertEquals(0, info1.getConnectCount());
        assertTrue(info1.isOnline());
        assertFalse(RebalanceUtil.isRebalancing(endpoint1));
    }

    @Test
    public void newCluster_DuplicateCluster_ShouldNotReinitialize() {
        // Given: Cluster initialized for the first time
        RebalanceUtil.newCluster(testEndpoints);
        EndpointInfo info1 = RebalanceUtil.getEndpointInfo(endpoint1);
        int initialCount = info1.getConnectCount();

        // When: Call newCluster again (same cluster)
        RebalanceUtil.newCluster(testEndpoints);

        // Then: EndpointInfo should not be reinitialized
        assertEquals(initialCount, RebalanceUtil.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void collectEndpointCountStats_NormalDistribution_ShouldReturnCorrectStats() {
        // Given: Cluster initialized with different connection counts
        RebalanceUtil.newCluster(testEndpoints);
        RebalanceUtil.incrementConnectionCount(endpoint1); // count=1
        RebalanceUtil.incrementConnectionCount(endpoint2);
        RebalanceUtil.incrementConnectionCount(endpoint2); // count=2
        RebalanceUtil.incrementConnectionCount(endpoint3);
        RebalanceUtil.incrementConnectionCount(endpoint3);
        RebalanceUtil.incrementConnectionCount(endpoint3); // count=3

        // When: Get min index (indirectly verify min count)
        int minIndex = RebalanceUtil.getMinConnectionEndpointIndex(connectionParam);
        // When: Invoke private needRebalancingInner via reflection (verify max/total count)
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(5); // Adjust threshold to meet condition
        boolean needRebalance = invokePrivateStaticMethod(
                RebalanceUtil.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        // Then: Verify stats via method results
        assertEquals(0, minIndex); // endpoint1 (count=1) is min
        assertTrue(needRebalance); // total=6>5 and max=3>1*(1.2)=1.2
    }

    @Test
    public void collectEndpointCountStats_EmptyEndpoints_ShouldReturnDefaultStats() {
        // Given: Empty endpoint list
        List<Endpoint> emptyEndpoints = new ArrayList<>();
        when(connectionParam.getEndpoints()).thenReturn(emptyEndpoints);

        // When: Get min index and invoke private needRebalancingInner
        int minIndex = RebalanceUtil.getMinConnectionEndpointIndex(connectionParam);
        boolean needRebalance = invokePrivateStaticMethod(
                RebalanceUtil.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        // Then: Verify default behavior
        assertEquals(-1, minIndex);
        assertFalse(needRebalance);
    }

    @Test
    public void collectEndpointCountStats_SameConnectionCount_ShouldReturnFirstMinIndex() {
        // Given: All endpoints have the same connection count (1)
        RebalanceUtil.newCluster(testEndpoints);
        RebalanceUtil.incrementConnectionCount(endpoint1);
        RebalanceUtil.incrementConnectionCount(endpoint2);
        RebalanceUtil.incrementConnectionCount(endpoint3);

        // When: Get min index
        int minIndex = RebalanceUtil.getMinConnectionEndpointIndex(connectionParam);

        // Then: minIndex should be 0 (first endpoint)
        assertEquals(0, minIndex);
    }

    @Test
    public void needRebalancingInner_ShouldRebalance_WhenThresholdMet() {
        // Given: Connection count distribution meets rebalancing conditions
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 3); // min=3
        setEndpointConnectCount(endpoint2, 4);
        setEndpointConnectCount(endpoint3, 5); // max=5, total=12
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10);
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);

        // When: Invoke private needRebalancingInner via reflection
        boolean result = invokePrivateStaticMethod(
                RebalanceUtil.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        // Then: Should return true (rebalancing needed)
        assertTrue(result);
    }

    @Test
    public void needRebalancingInner_ShouldNotRebalance_WhenTotalCountInsufficient() {
        // Given: Total connection count is below threshold (total=8<10)
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 2);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10);

        // When: Invoke private needRebalancingInner via reflection
        boolean result = invokePrivateStaticMethod(
                RebalanceUtil.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        // Then: Should return false (rebalancing not needed)
        assertFalse(result);
    }

    @Test
    public void needRebalancingInner_ShouldNotRebalance_WhenThresholdNotMet() {
        // Given: All endpoints have the same connection count (3 ≤ 3*(1+20%)=3.6)
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 3);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10);
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);

        // When: Invoke private needRebalancingInner via reflection
        boolean result = invokePrivateStaticMethod(
                RebalanceUtil.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        // Then: Should return false (rebalancing not needed)
        assertFalse(result);
    }

    @Test
    public void getMinConnectionEndpointIndex_ShouldReturnCorrectIndex() {
        // Given: Cluster initialized with endpoint2 having the minimum count (2)
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 5);
        setEndpointConnectCount(endpoint2, 2);
        setEndpointConnectCount(endpoint3, 4);

        // When: Call method to get min index
        int minIndex = RebalanceUtil.getMinConnectionEndpointIndex(connectionParam);

        // Then: Should return the index of endpoint2 (1)
        assertEquals(1, minIndex);
    }

    @Test
    public void handleRebalancing_RebalanceNeeded_ShouldSetRebalancingState() {
        // Given: Cluster initialized with rebalancing conditions met
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 10); // current endpoint (high count)
        setEndpointConnectCount(endpoint2, 3);  // min count
        setEndpointConnectCount(endpoint3, 5);
        RebalanceUtil.endpointUp(connectionParam, endpoint2);

        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10);
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);

        // When: Call handleRebalancing
        boolean result = RebalanceUtil.handleRebalancing(connectionParam, endpoint1);

        // Then: Should return true, and rebalancing state should be true
        assertTrue(result);
        assertTrue(RebalanceUtil.isRebalancing(endpoint1));
        assertTrue(RebalanceUtil.isRebalancing());
    }

    @Test
    public void handleRebalancing_RebalanceDone_ShouldResetRebalancingState() {
        // Given: Cluster is in rebalancing state, but conditions are no longer met
        RebalanceUtil.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 0);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(3);
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);

        // Manually trigger rebalancing state via public method
        RebalanceUtil.endpointUp(connectionParam, endpoint1);
        assertTrue(RebalanceUtil.isRebalancing(endpoint1));

        setEndpointConnectCount(endpoint1, 3);

        // When: Call handleRebalancing (conditions not met)
        boolean result = RebalanceUtil.handleRebalancing(connectionParam, endpoint1);

        // Then: Should return false, and rebalancing state should be reset
        assertFalse(result);
        assertFalse(RebalanceUtil.isRebalancing(endpoint1));
        assertFalse(RebalanceUtil.isRebalancing());
    }

    @Test
    public void incrementConnectionCount_ShouldIncreaseCount() {
        // Given: Initialized endpoint with default count (0)
        RebalanceUtil.newCluster(testEndpoints);
        EndpointInfo info = RebalanceUtil.getEndpointInfo(endpoint1);
        int initialCount = info.getConnectCount();

        // When: Call incrementConnectionCount
        RebalanceUtil.incrementConnectionCount(endpoint1);

        // Then: Connection count should be increased by 1
        assertEquals(initialCount + 1, RebalanceUtil.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void decrementConnectionCount_ShouldDecreaseCount() {
        // Given: Initialized endpoint with count=1
        RebalanceUtil.newCluster(testEndpoints);
        RebalanceUtil.incrementConnectionCount(endpoint1);
        int initialCount = RebalanceUtil.getEndpointInfo(endpoint1).getConnectCount();

        // When: Call decrementConnectionCount
        RebalanceUtil.decrementConnectionCount(endpoint1);

        // Then: Connection count should be decreased by 1
        assertEquals(initialCount - 1, RebalanceUtil.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void connected_ShouldMarkEndpointOnline() {
        // Given: Initialized endpoint (default offline state)
        RebalanceUtil.newCluster(testEndpoints);
        EndpointInfo info = RebalanceUtil.getEndpointInfo(endpoint1);
        assertTrue(info.isOnline());
        RebalanceUtil.disconnected(connectionParam, 0, null); // Mark offline for test

        // When: Call connected (index 0 → endpoint1)
        RebalanceUtil.connected(testEndpoints, 0);

        // Then: Endpoint should be marked as online
        assertTrue(RebalanceUtil.getEndpointInfo(endpoint1).isOnline());
    }

    @Test
    public void disconnected_ShouldMarkEndpointOfflineAndStartHealthCheck() {
        // Given: Initialized endpoint (online state, non-cloud environment)
        RebalanceUtil.newCluster(testEndpoints);
        RebalanceUtil.connected(testEndpoints, 0);
        setEndpointConnectCount(endpoint1, 1); // Set initial connection count
        assertTrue(RebalanceUtil.getEndpointInfo(endpoint1).isOnline());
        when(connectionParam.getCloudToken()).thenReturn(""); // Non-cloud environment

        // When: Call disconnected (InFlightRequest is null, test state only)
        RebalanceUtil.disconnected(connectionParam, 0, null);

        // Then: Endpoint should be marked as offline, connection count decreased
        assertFalse(RebalanceUtil.getEndpointInfo(endpoint1).isOnline());
        assertEquals(0, RebalanceUtil.getEndpointInfo(endpoint1).getConnectCount());
    }

    // ------------------------------ Utility method: Set endpoint connection count ------------------------------
    private void setEndpointConnectCount(Endpoint endpoint, int count) {
        for (int i = 0; i < count; i++) {
            RebalanceUtil.incrementConnectionCount(endpoint);
        }
    }
}