package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.ws.WSEWPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class WSEWPreparedStatementWriteTest extends AbstractWSEWPreparedStatementWriteTest {
    @Override
    protected String stmt2BindMode() {
        return "traditional";
    }

    @Override
    protected Class<?> expectedStatementClass() {
        return WSEWPreparedStatement.class;
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
