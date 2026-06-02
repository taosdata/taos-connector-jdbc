package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSEWPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class WSEWPreparedStatementWrite336Test extends AbstractWSEWPreparedStatementWriteTest {
    @Override
    protected String stmt2BindMode() {
        return "traditional";
    }

    @Override
    protected Class<?> expectedStatementClass() {
        return WSEWPreparedStatement.class;
    }

    @Override
    protected String stableInsertSql() {
        return WsStmtWriteTestSupport.stableInsertSql336(dbName, stableName);
    }

    @Override
    protected String asyncInsertSql() {
        return WsStmtWriteTestSupport.asyncInsertSql336(dbName, stableName);
    }

    @Override
    protected void createStable(java.sql.Statement statement) throws java.sql.SQLException {
        WsStmtWriteTestSupport.createAllTypeStable336(statement, dbName, stableName);
    }

    @Override
    protected void bindAllTypes(java.sql.PreparedStatement statement, int startIndex, long current)
            throws java.sql.SQLException {
        WsStmtWriteTestSupport.bindAllTypes336(statement, startIndex, current);
    }

    @Override
    protected void bindNullTypes(java.sql.PreparedStatement statement, int startIndex, long current)
            throws java.sql.SQLException {
        WsStmtWriteTestSupport.bindNullTypes336(statement, startIndex, current);
    }

    @Override
    protected void assertAllTypeRow(java.sql.ResultSet resultSet, long current) throws java.sql.SQLException {
        WsStmtWriteTestSupport.assertAllTypeRow336(resultSet, current);
    }

    @Override
    protected void assertNullTypeRow(java.sql.ResultSet resultSet, long current) throws java.sql.SQLException {
        WsStmtWriteTestSupport.assertNullTypeRow336(resultSet, current);
    }

    @BeforeClass
    public static void setUp336() {
        TestUtils.runIn336();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown336() {
        System.gc();
    }
}
