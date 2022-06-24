package com.taosdata.jdbc.tmq;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface Deserializer<T> extends Closeable {

    T deserialize(ResultSet data) throws InstantiationException, IllegalAccessException, SQLException;

    @Override
    default void close() {
    }
}
