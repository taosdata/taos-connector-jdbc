package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class WSEWColumnPreparedStatementTest {

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }

    @Test
    public void buildColumnBuffersFromQueuedRows_preservesQueuedTableOrder() throws Exception {
        List<Map<Integer, Column>> rows = Arrays.asList(
                row("d0", 1000L, 11),
                row("d1", 2000L, 12),
                row("d0", 3000L, 13));

        Stmt2ColumnFieldBuffer[] buffers = null;
        try {
            buffers = WSEWColumnPreparedStatement.buildColumnBuffersFromQueuedRows(rows, stmtInfo());

            assertEquals(3, buffers[0].getRowCount());
            assertEquals(3, buffers[1].getRowCount());
            assertEquals(3, buffers[2].getRowCount());
            assertEquals(3, buffers[0].computeTableCount());
        } finally {
            if (buffers != null) {
                for (Stmt2ColumnFieldBuffer buffer : buffers) {
                    if (buffer != null) {
                        buffer.release();
                    }
                }
            }
        }
    }

    @Test(expected = SQLException.class)
    public void buildColumnBuffersFromQueuedRows_rejectsMissingTbname() throws Exception {
        Map<Integer, Column> row = new HashMap<>();
        row.put(2, new Column(1000L, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(11, TSDB_DATA_TYPE_INT, 3));
        WSEWColumnPreparedStatement.buildColumnBuffersFromQueuedRows(
                Collections.singletonList(row),
                stmtInfo());
    }

    private static Map<Integer, Column> row(String tbname, long ts, int value) {
        Map<Integer, Column> row = new HashMap<>();
        row.put(1, new Column(tbname, TSDB_DATA_TYPE_BINARY, 1));
        row.put(2, new Column(ts, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(value, TSDB_DATA_TYPE_INT, 3));
        return row;
    }

    private static StmtInfo stmtInfo() {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setInsert(true);
        prepareResp.setStmtId(1L);
        prepareResp.setFields(Arrays.asList(tbnameField(), tsField(), valueField()));
        return new StmtInfo(prepareResp, "INSERT INTO ? VALUES (?, ?)");
    }

    private static Field tbnameField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_BINARY);
        return field;
    }

    private static Field tsField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        return field;
    }

    private static Field valueField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_INT);
        return field;
    }
}
