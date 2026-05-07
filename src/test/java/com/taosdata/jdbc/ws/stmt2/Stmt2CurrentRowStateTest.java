package com.taosdata.jdbc.ws.stmt2;

import org.junit.Test;

import static org.junit.Assert.*;

public class Stmt2CurrentRowStateTest {

    @Test
    public void stageFixed_stageVar_stageNull_andClear_updateSlots() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(3);

        assertTrue(state.isNull(0));
        assertTrue(state.isNull(1));
        assertTrue(state.isNull(2));

        state.stageFixed(0, 42L);
        state.stageVar(1, "abc".getBytes());
        state.stageNull(2);
        state.stageTbName("tb_01");

        assertFalse(state.isNull(0));
        assertEquals(42L, state.fixedValue(0));
        assertNull(state.varValue(0));

        assertFalse(state.isNull(1));
        assertEquals(0L, state.fixedValue(1));
        assertArrayEquals("abc".getBytes(), state.varValue(1));

        assertTrue(state.isNull(2));
        assertEquals(0L, state.fixedValue(2));
        assertNull(state.varValue(2));

        assertEquals("tb_01", state.tableName());
        assertTrue(state.hasPendingValues());

        state.clear();

        assertTrue(state.isNull(0));
        assertTrue(state.isNull(1));
        assertTrue(state.isNull(2));
        assertEquals(0L, state.fixedValue(0));
        assertNull(state.varValue(1));
        assertNull(state.tableName());
        assertFalse(state.hasPendingValues());
    }

    @Test
    public void hasPendingValues_trueWhenAnySlotOrTbNameIsStaged() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(2);

        assertFalse(state.hasPendingValues());

        state.stageTbName("tb");
        assertTrue(state.hasPendingValues());

        state.clear();
        state.stageNull(0);
        assertTrue(state.hasPendingValues());

        state.stageFixed(1, 7L);
        assertTrue(state.hasPendingValues());
    }

    @Test
    public void hasPendingValues_trueWhenNullWasExplicitlyStaged() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(1);

        assertFalse(state.hasPendingValues());

        state.stageNull(0);
        assertTrue(state.hasPendingValues());

        state.clear();
        assertFalse(state.hasPendingValues());
    }
}
