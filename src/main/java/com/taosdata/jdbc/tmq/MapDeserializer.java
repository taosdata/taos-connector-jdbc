package com.taosdata.jdbc.tmq;

import com.google.common.collect.Maps;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class MapDeserializer implements Deserializer<Map<String, Object>> {
    @Override
    public Map<String, Object> deserialize(ResultSet data, String topic, String dbName) throws SQLException {
        ResultSetMetaData metaData = data.getMetaData();
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            map.put(metaData.getColumnLabel(i), data.getObject(i));
        }

        return map;
    }
}
