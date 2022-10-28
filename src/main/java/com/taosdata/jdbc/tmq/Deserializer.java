package com.taosdata.jdbc.tmq;

import java.beans.IntrospectionException;
import java.io.Closeable;
import java.sql.ResultSet;
import java.util.Map;

public interface Deserializer<V> extends Closeable {

    default void configure(Map<?, ?> configs)  throws IntrospectionException {
        // intentionally left blank
    }

    V deserialize(ResultSet data) throws Exception;

    @Override
    default void close() {
    }
}
