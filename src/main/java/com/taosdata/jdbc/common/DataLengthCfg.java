package com.taosdata.jdbc.common;

public class DataLengthCfg {
    private static final Integer[] dataLenArr = {
            1,//TSDB_DATA_TYPE_NULL
            1,//TSDB_DATA_TYPE_BOOL
            1,//TSDB_DATA_TYPE_TINYINT
            2,//TSDB_DATA_TYPE_SMALLINT
            4,//TSDB_DATA_TYPE_INT
            8,//TSDB_DATA_TYPE_BIGINT
            4,//TSDB_DATA_TYPE_FLOAT
            8,//TSDB_DATA_TYPE_DOUBLE
            null,
            8//TSDB_DATA_TYPE_TIMESTAMP
    };

    public static Integer getDataLength(int dataType){
        if (dataType < dataLenArr.length){
            return dataLenArr[dataType];
        }
        return null;
    }
}
