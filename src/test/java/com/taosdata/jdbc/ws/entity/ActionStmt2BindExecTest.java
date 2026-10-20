/*
 * Copyright (c) 2026 TAOS Data, Inc.
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
