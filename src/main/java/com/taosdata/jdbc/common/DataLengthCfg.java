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
            8, //TSDB_DATA_TYPE_TIMESTAMP
            null,
            1, //TSDB_DATA_TYPE_UTINYINT
            2, //TSDB_DATA_TYPE_USMALLINT
            4, //TSDB_DATA_TYPE_UINT
            8, //TSDB_DATA_TYPE_UBIGINT
            null, //TSDB_DATA_TYPE_JSON
            null, //TSDB_DATA_TYPE_VARBINARY
            16, //TSDB_DATA_TYPE_DECIMAL128
            null, //TSDB_DATA_TYPE_BLOB
            null, //TSDB_DATA_TYPE_MEDIUMBLOB
            null, //TSDB_DATA_TYPE_GEOMETRY
            8 //TSDB_DATA_TYPE_DECIMAL64
};

    public static Integer getDataLength(int dataType){
        if (dataType < dataLenArr.length){
            return dataLenArr[dataType];
        }
        return null;
    }
}
