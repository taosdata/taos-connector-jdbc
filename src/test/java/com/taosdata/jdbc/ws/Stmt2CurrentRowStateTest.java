package com.taosdata.jdbc.ws;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class Stmt2CurrentRowStateTest {

    @Test
    public void stageFixed_stageVar_stageNull_andClear_updateSlots() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(4);

        assertTrue(state.isNull(0));
        assertTrue(state.isNull(1));
        assertTrue(state.isNull(2));
        assertTrue(state.isNull(3));

        state.stageFixed1(0, (byte) 42);
        state.stageVar(1, "abc".getBytes());
        state.stageNull(2);
        state.stageString(3, "tb_01");

        assertFalse(state.isNull(0));
        assertEquals(42, state.fixed1Value(0));
        assertNull(state.varValue(0));

        assertFalse(state.isNull(1));
        assertArrayEquals("abc".getBytes(), state.varValue(1));

        assertTrue(state.isNull(2));
        assertNull(state.varValue(2));

        assertFalse(state.isNull(3));
        assertEquals("tb_01", state.stringValue(3));
        assertNull(state.varValue(3));
        assertTrue(state.hasPendingValues());

        state.clear();

        assertTrue(state.isNull(0));
        assertTrue(state.isNull(1));
        assertTrue(state.isNull(2));
        assertTrue(state.isNull(3));
        assertNull(state.varValue(1));
        assertNull(state.stringValue(3));
        assertFalse(state.hasPendingValues());
    }

    @Test
    public void stageString_preservesStringUntilFlushAndClear() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(2);

        state.stageString(0, "alpha");
        state.stageVar(1, "bytes".getBytes());

        assertEquals("alpha", state.stringValue(0));
        assertNull(state.varValue(0));
        assertArrayEquals("bytes".getBytes(), state.varValue(1));
        assertNull(state.stringValue(1));

        state.clear();

        assertNull(state.stringValue(0));
        assertNull(state.varValue(1));
        assertFalse(state.hasPendingValues());
    }

    @Test
    public void hasPendingValues_trueWhenAnySlotOrTbNameIsStaged() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(2);

        assertFalse(state.hasPendingValues());

        state.stageString(0, "tb");
        assertTrue(state.hasPendingValues());

        state.clear();
        state.stageNull(0);
        assertTrue(state.hasPendingValues());

        state.stageFixed8(1, 7L);
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

    @Test
    public void stageVar_recordsLengthAndClearsReferenceOnReset() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(2);
        byte[] utf8 = "meters".getBytes(StandardCharsets.UTF_8);

        state.stageVar(0, utf8, utf8.length);
        state.stageNull(1);

        assertFalse(state.isNull(0));
        assertArrayEquals(utf8, state.varValue(0));
        assertEquals(utf8.length, state.varLength(0));
        assertTrue(state.hasPendingValues());
        assertTrue(state.isRowDirty());

        state.clear();

        assertTrue(state.isNull(0));
        assertNull(state.varValue(0));
        assertEquals(0, state.varLength(0));
        assertFalse(state.hasPendingValues());
        assertFalse(state.isRowDirty());
    }

    @Test
    public void explicitNullMakesRowDirtyWithoutCreatingNonNullValue() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(1);

        state.stageNull(0);

        assertTrue(state.isNull(0));
        assertTrue(state.hasPendingValues());
        assertTrue(state.isRowDirty());
    }

    @Test
    public void stageFixed4AndFixed8PreserveRawBits() {
        Stmt2CurrentRowState state = new Stmt2CurrentRowState(2);

        state.stageFixed4(0, Float.floatToRawIntBits(3.14f));
        state.stageFixed8(1, Double.doubleToRawLongBits(Math.PI));

        assertEquals(Float.floatToRawIntBits(3.14f), state.fixed4Value(0));
        assertEquals(Double.doubleToRawLongBits(Math.PI), state.fixed8Value(1));
    }
}
