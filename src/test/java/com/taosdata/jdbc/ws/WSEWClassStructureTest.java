package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WSEWClassStructureTest {

    @Test
    public void legacyWSEW_extendsAbstractWSEWBase() {
        assertEquals(AbstractWSEWPreparedStatement.class, WSEWPreparedStatement.class.getSuperclass());
    }

    @Test
    public void columnarWSEW_extendsAbstractWSEWBase() {
        assertEquals(AbstractWSEWPreparedStatement.class, WSEWColumnPreparedStatement.class.getSuperclass());
    }
}
