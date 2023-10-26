package com.taosdata.jdbc.common;

import java.util.HashMap;

import static com.taosdata.jdbc.TSDBConstants.*;

public class DataLengthCfg {
    private static final HashMap<Integer, Integer> dataLengthMap= new HashMap<Integer, Integer>(){{
        put(TSDB_DATA_TYPE_NULL, 1);
        put(TSDB_DATA_TYPE_BOOL, 1);
        put(TSDB_DATA_TYPE_TINYINT, 1);
        put(TSDB_DATA_TYPE_SMALLINT, 2);
        put(TSDB_DATA_TYPE_INT, 4);
        put(TSDB_DATA_TYPE_BIGINT, 8);
        put(TSDB_DATA_TYPE_FLOAT, 4);
        put(TSDB_DATA_TYPE_DOUBLE, 8);
        put(TSDB_DATA_TYPE_TIMESTAMP, 8);
    }};
    public static Integer getDataLength(int dataType){
        return dataLengthMap.get(dataType);
    }
}
