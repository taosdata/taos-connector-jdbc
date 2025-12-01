package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.tmq.WSConsumerResultSet;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;
public class WSConsumerResultSetTest {

    private WSConsumerResultSet wsConsumerResultSet;

    @Before
    public void setUp() {
        wsConsumerResultSet = new WSConsumerResultSet(null, null, 0, null, null);
    }

    @Test
    public void testIsBeforeFirst() throws SQLException {
        assertFalse(wsConsumerResultSet.isBeforeFirst());
    }

    @Test
    public void testIsAfterLast() throws SQLException {
        assertFalse(wsConsumerResultSet.isAfterLast());
    }

    @Test
    public void testIsFirst() throws SQLException {
        assertTrue(wsConsumerResultSet.isFirst());
    }

    @Test
    public void testIsLast() throws SQLException {
        assertFalse(wsConsumerResultSet.isLast());
    }

    @Test
    public void testBeforeFirst() throws SQLException {
        wsConsumerResultSet.beforeFirst();
    }

    @Test
    public void testafterFirst() throws SQLException {
        wsConsumerResultSet.afterLast();
    }

    @Test
    public void testFirst() throws SQLException {
        assertFalse(wsConsumerResultSet.first());
    }


    @Test
    public void testLast() throws SQLException {
        assertFalse(wsConsumerResultSet.last());
    }

    @Test
    public void testGetRow() throws SQLException {
        assertEquals(0, wsConsumerResultSet.getRow());
    }

    @Test(expected = SQLException.class)
    public void testAbsolute() throws SQLException {
        wsConsumerResultSet.absolute(0);
    }

    @Test(expected = SQLException.class)
    public void testRelative() throws SQLException {
        wsConsumerResultSet.relative(0);
    }

    @Test(expected = SQLException.class)
    public void testPrevious() throws SQLException {
        wsConsumerResultSet.previous();
    }

}
