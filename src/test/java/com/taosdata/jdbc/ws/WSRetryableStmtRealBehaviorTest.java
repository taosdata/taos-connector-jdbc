package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Real behavior tests for WSRetryableStmt bind execution routing (Task 1 final verification).
 * 
 * These tests execute the ACTUAL WSRetryableStmt.writeBlockWithRetrySync() method on the real write path
 * and verify which transport actions were sent, proving the routing behavior without reflection-based introspection.
 * 
 * Test guarantees verified:
 * 1. Default/public constructor → sends STMT2_BIND + STMT2_EXEC (never STMT2_BIND_EXEC)
 * 2. Internal bind-exec requested but server unsupported → sends STMT2_BIND + STMT2_EXEC (compatibility gate enforced)
 * 3. Internal bind-exec requested and server supported → sends STMT2_BIND_EXEC only
 * 4. Non-WSConnection → sends STMT2_BIND + STMT2_EXEC (never STMT2_BIND_EXEC)
 * 
 * Implementation approach:
 * - FakeTransport extends Transport (using protected no-arg constructor from same package)
 * - Overrides send(...) methods to record action strings
 * - Returns minimal valid responses to allow the real write path to complete
 * - Uses reflection only to inject mock WSConnectionManager (to satisfy final getConnectionParam() calls)
 * - Assertions are on recorded actions AFTER invoking real WSRetryableStmt.writeBlockWithRetrySync()
 */
public class WSRetryableStmtRealBehaviorTest {

    @BeforeClass
    public static void setUpClass() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDownClass() {
        System.gc();
    }

    /**
     * Fake Transport that records all actions sent through it.
     * This allows us to verify the actual behavior of WSRetryableStmt without relying on reflection
     * for the test assertions (though we use reflection to inject mock connectionManager).
     */
    static class FakeTransport extends Transport {
        private final List<String> recordedActions = new ArrayList<>();
        private int reconnectCount = 0;

        public FakeTransport(ConnectionParam param) throws Exception {
            super(); // Use protected no-arg constructor
            
            // Use reflection to inject a mock connectionManager that supports the final methods
            java.lang.reflect.Field cmField = Transport.class.getDeclaredField("connectionManager");
            cmField.setAccessible(true);
            
            WSConnectionManager mockManager = mock(WSConnectionManager.class);
            when(mockManager.getConnectionParam()).thenReturn(param);
            when(mockManager.getReconnectCount()).thenReturn(reconnectCount);
            when(mockManager.isConnected()).thenReturn(true);
            
            // Mock getCurrentEndpoint to return a valid endpoint
            com.taosdata.jdbc.common.Endpoint mockEndpoint = mock(com.taosdata.jdbc.common.Endpoint.class);
            when(mockManager.getCurrentEndpoint()).thenReturn(mockEndpoint);
            
            cmField.set(this, mockManager);
        }

        @Override
        public Response send(String action, long reqId, ByteBuf buffer, boolean resend, long timeout) throws SQLException {
            recordedActions.add(action);
            
            // Return appropriate response based on action
            if ("stmt2_bind".equals(action)) {
                Stmt2Resp resp = new Stmt2Resp();
                resp.setCode(Code.SUCCESS.getCode());
                resp.setStmtId(12345L);
                return resp;
            } else if ("stmt2_bind_exec".equals(action)) {
                Stmt2ExecResp resp = new Stmt2ExecResp();
                resp.setCode(Code.SUCCESS.getCode());
                resp.setStmtId(12345L);
                resp.setAffected(10);
                return resp;
            }
            
            throw new SQLException("Unexpected action: " + action);
        }

        @Override
        public Response send(Request request, boolean resend, long timeout) throws SQLException {
            recordedActions.add(request.getAction());
            
            // Return appropriate response based on action
            if ("stmt2_exec".equals(request.getAction())) {
                Stmt2ExecResp resp = new Stmt2ExecResp();
                resp.setCode(Code.SUCCESS.getCode());
                resp.setStmtId(12345L);
                resp.setAffected(10);
                return resp;
            }
            
            throw new SQLException("Unexpected action: " + request.getAction());
        }

        @Override
        public int getReconnectCount() {
            return reconnectCount;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        public List<String> getRecordedActions() {
            return new ArrayList<>(recordedActions);
        }

        public void clearRecordedActions() {
            recordedActions.clear();
        }
    }

    private ConnectionParam mockConnectionParam;
    private AbstractConnection mockConnection;
    private StmtInfo stmtInfo;
    private AtomicInteger batchInsertedRows;

    @Before
    public void setUp() {
        // Create mock connection param with reasonable defaults
        mockConnectionParam = mock(ConnectionParam.class);
        when(mockConnectionParam.isEnableAutoConnect()).thenReturn(false);
        when(mockConnectionParam.getRetryTimes()).thenReturn(1);
        when(mockConnectionParam.getRequestTimeout()).thenReturn(5000);
        when(mockConnectionParam.getZoneId()).thenReturn(java.time.ZoneId.systemDefault());

        // Create mock connection
        mockConnection = mock(AbstractConnection.class);

        // Create stmt info (requires SQL in constructor)
        stmtInfo = new StmtInfo("INSERT INTO test VALUES(?, ?)");
        stmtInfo.setStmtId(12345L);

        batchInsertedRows = new AtomicInteger(0);
    }

    /**
     * Test 1: Default/public constructor path sends STMT2_BIND then STMT2_EXEC,
     * and does NOT send STMT2_BIND_EXEC
     */
    @Test
    public void testDefaultConstructorUsesLegacyBindExecPath() throws Exception {
        // Arrange
        FakeTransport transport = new FakeTransport(mockConnectionParam);
        
        // Use public constructor (without useBindExec parameter)
        WSRetryableStmt stmt = new WSRetryableStmt(
                mockConnection,
                mockConnectionParam,
                "test_db",
                transport,
                1L,
                stmtInfo,
                batchInsertedRows
        );

        // Create a dummy buffer (content doesn't matter for this test)
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L); // reqId placeholder
        rawBlock.writeLongLE(12345L); // stmtId placeholder
        rawBlock.writeIntLE(1); // dummy data

        // Act
        transport.clearRecordedActions();
        stmt.writeBlockWithRetrySync(rawBlock);

        // Assert
        List<String> actions = transport.getRecordedActions();
        assertEquals("Should send exactly 2 actions", 2, actions.size());
        assertEquals("First action should be stmt2_bind", "stmt2_bind", actions.get(0));
        assertEquals("Second action should be stmt2_exec", "stmt2_exec", actions.get(1));
        assertFalse("Should NOT send stmt2_bind_exec", actions.contains("stmt2_bind_exec"));
        
        // Verify batch counter was updated
        assertEquals("Batch inserted rows should be updated", 10, batchInsertedRows.get());
        
        // Clean up
        rawBlock.release();
    }

    /**
     * Test 2: Internal bind-exec requested but server unsupported
     * still sends legacy STMT2_BIND then STMT2_EXEC
     */
    @Test
    public void testBindExecRequestedButServerUnsupportedUsesLegacyPath() throws Exception {
        // Arrange
        FakeTransport transport = new FakeTransport(mockConnectionParam);
        
        // Mock WSConnection that does NOT support stmt2_bind_exec
        WSConnection mockWsConnection = mock(WSConnection.class);
        when(mockWsConnection.supportsStmt2BindExec()).thenReturn(false);

        // Use package-private constructor with useBindExec=true
        // but server doesn't support it
        WSRetryableStmt stmt = new WSRetryableStmt(
                mockWsConnection,
                mockConnectionParam,
                "test_db",
                transport,
                1L,
                stmtInfo,
                batchInsertedRows,
                true  // Request bind-exec mode
        );

        // Verify the stmt is NOT using bind-exec (compatibility gate enforced)
        assertFalse("Should fall back to legacy mode when server doesn't support bind-exec",
                stmt.isUsingBindExec());

        // Create a dummy buffer
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L); // reqId placeholder
        rawBlock.writeLongLE(12345L); // stmtId placeholder
        rawBlock.writeIntLE(1); // dummy data

        // Act
        transport.clearRecordedActions();
        stmt.writeBlockWithRetrySync(rawBlock);

        // Assert
        List<String> actions = transport.getRecordedActions();
        assertEquals("Should send exactly 2 actions", 2, actions.size());
        assertEquals("First action should be stmt2_bind", "stmt2_bind", actions.get(0));
        assertEquals("Second action should be stmt2_exec", "stmt2_exec", actions.get(1));
        assertFalse("Should NOT send stmt2_bind_exec", actions.contains("stmt2_bind_exec"));
        
        // Clean up
        rawBlock.release();
    }

    /**
     * Test 3: Internal bind-exec requested and server supported
     * sends STMT2_BIND_EXEC (single action)
     */
    @Test
    public void testBindExecRequestedAndServerSupportedUsesBindExecPath() throws Exception {
        // Arrange
        FakeTransport transport = new FakeTransport(mockConnectionParam);
        
        // Mock WSConnection that DOES support stmt2_bind_exec
        WSConnection mockWsConnection = mock(WSConnection.class);
        when(mockWsConnection.supportsStmt2BindExec()).thenReturn(true);

        // Use package-private constructor with useBindExec=true
        WSRetryableStmt stmt = new WSRetryableStmt(
                mockWsConnection,
                mockConnectionParam,
                "test_db",
                transport,
                1L,
                stmtInfo,
                batchInsertedRows,
                true  // Request bind-exec mode
        );

        // Verify the stmt IS using bind-exec
        assertTrue("Should use bind-exec mode when server supports it",
                stmt.isUsingBindExec());

        // Create a dummy buffer
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L); // reqId placeholder
        rawBlock.writeLongLE(12345L); // stmtId placeholder
        rawBlock.writeIntLE(1); // dummy data

        // Act
        transport.clearRecordedActions();
        stmt.writeBlockWithRetrySync(rawBlock);

        // Assert
        List<String> actions = transport.getRecordedActions();
        assertEquals("Should send exactly 1 action", 1, actions.size());
        assertEquals("Should send stmt2_bind_exec", "stmt2_bind_exec", actions.get(0));
        assertFalse("Should NOT send stmt2_bind", actions.contains("stmt2_bind"));
        assertFalse("Should NOT send stmt2_exec", actions.contains("stmt2_exec"));
        
        // Verify batch counter was updated
        assertEquals("Batch inserted rows should be updated", 10, batchInsertedRows.get());
        
        // Clean up
        rawBlock.release();
    }

    /**
     * Test 4: Verify non-WSConnection still defaults to legacy path
     */
    @Test
    public void testNonWSConnectionAlwaysUsesLegacyPath() throws Exception {
        // Arrange
        FakeTransport transport = new FakeTransport(mockConnectionParam);
        
        // Use non-WSConnection (just AbstractConnection)
        AbstractConnection mockAbstractConnection = mock(AbstractConnection.class);

        // Use package-private constructor with useBindExec=true
        // but connection is not WSConnection
        WSRetryableStmt stmt = new WSRetryableStmt(
                mockAbstractConnection,
                mockConnectionParam,
                "test_db",
                transport,
                1L,
                stmtInfo,
                batchInsertedRows,
                true  // Request bind-exec mode
        );

        // Verify the stmt is NOT using bind-exec
        assertFalse("Should fall back to legacy mode for non-WSConnection",
                stmt.isUsingBindExec());

        // Create a dummy buffer
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L); // reqId placeholder
        rawBlock.writeLongLE(12345L); // stmtId placeholder
        rawBlock.writeIntLE(1); // dummy data

        // Act
        transport.clearRecordedActions();
        stmt.writeBlockWithRetrySync(rawBlock);

        // Assert
        List<String> actions = transport.getRecordedActions();
        assertEquals("Should send exactly 2 actions", 2, actions.size());
        assertEquals("First action should be stmt2_bind", "stmt2_bind", actions.get(0));
        assertEquals("Second action should be stmt2_exec", "stmt2_exec", actions.get(1));
        
        // Clean up
        rawBlock.release();
    }
}
