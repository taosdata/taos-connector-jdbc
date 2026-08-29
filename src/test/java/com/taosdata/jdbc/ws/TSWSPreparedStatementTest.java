package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.SQLException;

/**
 * TSWSPreparedStatement suite for the current engine, run with the default
 * statement cache. Test methods live in {@link TSWSPreparedStatementTestBase}.
 */
public class TSWSPreparedStatementTest extends TSWSPreparedStatementTestBase {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestUtils.runInMain();
        dbName = TestUtils.camelToSnake(TSWSPreparedStatementTest.class);
        setUpDatabase("");
    }

    @AfterClass
    public static void afterClass() {
        tearDownDatabase();
    }
}
