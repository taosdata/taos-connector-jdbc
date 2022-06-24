package com.taosdata.jdbc.tmq;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MapDeserializer implements Deserializer<Map<String, Object>> {
    @Override
    public Map<String, Object> deserialize(ResultSet data) {
        Map<String,Object> map = new HashMap<>();

        try {
            ResultSetMetaData metaData = data.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                map.put(metaData.getColumnName(i),data.getObject(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return map;
    }
}
