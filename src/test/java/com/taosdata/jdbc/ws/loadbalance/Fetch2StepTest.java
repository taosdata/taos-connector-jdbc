package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.RebalanceUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.FetchBlockHealthCheckResp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class Fetch2StepTest {

    @Mock
    private BgHealthCheck context;
    @Mock
    private WSClient wsClient;
    @Mock
    private InFlightRequest inFlightRequest;
    @Mock
    private ConnectionParam param;
    @Mock
    private StepFlow flow;
    private Fetch2Step fetch2Step;
    private final Endpoint endpoint = new Endpoint("test-endpoint", 6041, false);
    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        fetch2Step = new Fetch2Step();

        when(context.getWsClient()).thenReturn(wsClient);
        when(context.getInFlightRequest()).thenReturn(inFlightRequest);
        when(context.getParam()).thenReturn(param);
        when(context.getEndpoint()).thenReturn(endpoint);
    }

    /**
     * Test: When WebSocket client is not open, return CONNECT step
     */
    @Test
    public void execute_wsClientNotOpen_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(false);
        int expectedInterval = 5;
        when(context.getNextInterval()).thenReturn(expectedInterval);

        // Act
        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(wsClient, never()).send(any(ByteBuf.class));
        verify(inFlightRequest, never()).put(any(FutureResponse.class));
    }

    /**
     * Test: When inFlightRequest.put throws exception, return CONNECT step with 0 interval
     */
    @Test
    public void execute_putThrowsException_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        doThrow(new IllegalStateException("Cache full")).when(inFlightRequest).put(any(FutureResponse.class));

        // Act
        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(wsClient).send(any(ByteBuf.class));
    }

    /**
     * Test: When response is completed (isCompleted()=true), return FINISH step and call RebalanceUtil.endpintUp
     */
    @Test
    public void execute_responseCompleted_returnsFinishStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Complete with completed response
        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);

        byte[] buffer = StringUtils.hexToBytes("070000000000000001003ac0060000000000050030be0629e4230000000000000000010000000000000001");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp completedResp = new FetchBlockHealthCheckResp(buf);
        completedResp.setCompleted(true);
        try (MockedStatic<RebalanceUtil> mockedRebalance = mockStatic(RebalanceUtil.class)) {
            futureResponseCaptor.getValue().getFuture().complete(completedResp);

            // Assert
            StepResponse response = future.get();
            Assert.assertEquals(StepEnum.FINISH, response.getStep());
            Assert.assertEquals(0, response.getWaitSeconds());
            verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
            verify(context, never()).cleanUp();
            mockedRebalance.verify(() -> RebalanceUtil.endpointUp(context.getParam(), endpoint), times(1));
        }
    }

    /**
     * Test: When response is not completed (isCompleted()=false), return FREE_RESULT step
     */
    @Test
    public void execute_responseNotCompleted_returnsFreeResultStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Complete with not completed response
        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        byte[] buffer = StringUtils.hexToBytes("07000000000000000100fac7070000000000050030be0629e42300000000000000000100000000000000002b000000010000002b0000000100000001000000000000800000000000000000040400000004000000000100000000");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);

        FetchBlockHealthCheckResp notCompletedResp = new FetchBlockHealthCheckResp(buf);
        notCompletedResp.setCompleted(false);
        try (MockedStatic<RebalanceUtil> mockedRebalance = mockStatic(RebalanceUtil.class)) {
            futureResponseCaptor.getValue().getFuture().complete(notCompletedResp);

            // Assert
            StepResponse response = future.get();
            Assert.assertEquals(StepEnum.FETCH2, response.getStep());
            Assert.assertEquals(0, response.getWaitSeconds());
            verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
            mockedRebalance.verify(() -> RebalanceUtil.endpointUp(any(), any()), never());
            verify(context, never()).cleanUp();
        }
    }

    /**
     * Test: When response throws exception (e.g., timeout), return FREE_RESULT step with recovery interval
     */
    @Test
    public void execute_responseException_returnsFreeResultWithRecovery() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        int expectedInterval = 10;
        when(context.getRecoveryInterval()).thenReturn(expectedInterval);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Trigger exception
        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        SQLTimeoutException testException = new SQLTimeoutException("Request timed out");
        futureResponseCaptor.getValue().getFuture().completeExceptionally(testException);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.FREE_RESULT, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(context).cleanUp();
    }
    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}