package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for STMT2_BIND_EXEC action enum entry.
 * Verifies that the new action is properly mapped and configured.
 */
public class ActionStmt2BindExecTest {

    /**
     * Test that STMT2_BIND_EXEC action exists and has correct properties
     */
    @Test
    public void testStmt2BindExecActionExists() {
        Action action = Action.STMT2_BIND_EXEC;
        
        assertNotNull("STMT2_BIND_EXEC action should exist", action);
        assertEquals("stmt2_bind_exec", action.getAction());
        assertEquals(Stmt2ExecResp.class, action.getResponseClazz());
    }

    /**
     * Test that STMT2_BIND_EXEC can be retrieved by action string
     */
    @Test
    public void testStmt2BindExecActionOf() {
        Action action = Action.of("stmt2_bind_exec");
        
        assertNotNull("Action.of should return non-null for stmt2_bind_exec", action);
        assertEquals(Action.STMT2_BIND_EXEC, action);
    }

    /**
     * Test that STMT2_BIND_EXEC uses same response class as STMT2_EXEC
     */
    @Test
    public void testStmt2BindExecResponseClass() {
        Action bindExecAction = Action.STMT2_BIND_EXEC;
        Action execAction = Action.STMT2_EXEC;
        
        assertEquals("STMT2_BIND_EXEC should use same response class as STMT2_EXEC",
                execAction.getResponseClazz(), bindExecAction.getResponseClazz());
    }

    /**
     * Test that all stmt2 actions are distinct
     */
    @Test
    public void testStmt2ActionsAreDistinct() {
        Action init = Action.STMT2_INIT;
        Action prepare = Action.STMT2_PREPARE;
        Action bind = Action.STMT2_BIND;
        Action exec = Action.STMT2_EXEC;
        Action bindExec = Action.STMT2_BIND_EXEC;
        Action close = Action.STMT2_CLOSE;
        Action useResult = Action.STMT2_USE_RESULT;

        assertNotEquals(init, prepare);
        assertNotEquals(bind, exec);
        assertNotEquals(exec, bindExec);
        assertNotEquals(bindExec, bind);
        
        // Verify action strings are different
        assertNotEquals(bind.getAction(), bindExec.getAction());
        assertNotEquals(exec.getAction(), bindExec.getAction());
    }
}
