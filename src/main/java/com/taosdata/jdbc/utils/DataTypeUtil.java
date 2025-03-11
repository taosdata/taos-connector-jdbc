package com.taosdata.jdbc.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.enums.TimestampPrecision;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;

public class DataTypeUtil {
    public static String getColumnClassName(int type) {
        String columnClassName = "";
        switch (type) {
            case Types.NULL:
                columnClassName = "java.lang.Object";
                break;
            case Types.TIMESTAMP:
                columnClassName = Timestamp.class.getName();
                break;
            case Types.NCHAR:
                columnClassName = String.class.getName();
                break;
            case Types.DOUBLE:
                columnClassName = Double.class.getName();
                break;
            case Types.FLOAT:
                columnClassName = Float.class.getName();
                break;
            case Types.BIGINT:
                columnClassName = Long.class.getName();
                break;
            case Types.INTEGER:
                columnClassName = Integer.class.getName();
                break;
            case Types.SMALLINT:
                columnClassName = Short.class.getName();
                break;
            case Types.TINYINT:
                columnClassName = Byte.class.getName();
                break;
            case Types.BOOLEAN:
                columnClassName = Boolean.class.getName();
                break;
            case Types.BINARY:
            case Types.VARCHAR:
            case Types.VARBINARY:
                columnClassName = "[B";
                break;
        }
        return columnClassName;
    }
}
