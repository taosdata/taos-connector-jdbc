package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSEWColumnPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class WSEWColumnPreparedStatementWriteTest extends AbstractWSEWPreparedStatementWriteTest {
    @Override
    protected String stmt2BindMode() {
        return "column";
    }

    @Override
    protected Class<?> expectedStatementClass() {
        return WSEWColumnPreparedStatement.class;
    }

    @BeforeClass
    public static void setUp() {
        TestUtils.runInMain();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
