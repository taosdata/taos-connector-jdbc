package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;

import java.sql.SQLException;
import java.util.List;

public class WSConsumerResultSetMetaData extends RestfulResultSetMetaData {

    private final String tableName;

    public WSConsumerResultSetMetaData(String database, List<RestfulResultSet.Field> fields, String tableName) {
        super(database, fields);
        this.tableName = tableName;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName;
    }
}
