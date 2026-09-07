package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.SQLException;

/**
 * TSWSPreparedStatement suite for TDengine 3.3.6, run with the statement
 * cache disabled (stmtCacheSize=0): the suite recreates tables every round,
 * which 3.3.6 cannot combine with cached statements. Test methods live in
 * {@link TSWSPreparedStatementTestBase}.
 */
public class TSWSPreparedStatement336Test extends TSWSPreparedStatementTestBase {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestUtils.runIn336();
        dbName = TestUtils.camelToSnake(TSWSPreparedStatement336Test.class);
        setUpDatabase("&stmtCacheSize=0");
    }

    @AfterClass
    public static void afterClass() {
        tearDownDatabase();
    }
}
