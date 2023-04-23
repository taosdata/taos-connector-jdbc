package com.taosdata.jdbc.common;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;

public class SerializeBlockTest {

    @Test
    public void getRawBlock() throws SQLException, IOException {
        byte[] bytes = new byte[]{
        };

        List<ColumnInfo> list = new ArrayList<>();
        list.add(new ColumnInfo(1, new Timestamp(0), TSDB_DATA_TYPE_TIMESTAMP));
        list.add(new ColumnInfo(2,true, TSDB_DATA_TYPE_BOOL));
        list.add(new ColumnInfo(3, (byte) 127, TSDB_DATA_TYPE_TINYINT));
        list.add(new ColumnInfo(4, (short) 32767, TSDB_DATA_TYPE_SMALLINT));
        list.add(new ColumnInfo(5, 2147483647, TSDB_DATA_TYPE_INT));
        list.add(new ColumnInfo(6, 9223372036854775807L, TSDB_DATA_TYPE_BIGINT));
        list.add(new ColumnInfo(7, Float.MAX_VALUE, TSDB_DATA_TYPE_FLOAT));
        list.add(new ColumnInfo(8, Double.MAX_VALUE, TSDB_DATA_TYPE_DOUBLE));
        list.add(new ColumnInfo(9, "ABC".getBytes(StandardCharsets.UTF_8), TSDB_DATA_TYPE_BINARY));
        list.add(new ColumnInfo(10, "涛思数据", TSDB_DATA_TYPE_NCHAR));
        byte[] rawBlock = SerializeBlock.getRawBlock(list);
        Assert.assertArrayEquals(rawBlock, bytes);
    }
}