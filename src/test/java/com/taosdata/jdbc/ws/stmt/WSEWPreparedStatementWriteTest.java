package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.ws.WSEWColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSEWPreparedStatement;
import com.taosdata.jdbc.utils.TestUtils;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WSEWPreparedStatementWriteTest extends AbstractWSEWPreparedStatementWriteTest {
    public WSEWPreparedStatementWriteTest(String stmt2BindMode, Class<?> expectedStatementClass) {
        super(stmt2BindMode, expectedStatementClass);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"traditional", WSEWPreparedStatement.class},
                {"column", WSEWColumnPreparedStatement.class}
        });
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
