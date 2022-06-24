package com.taosdata.jdbc.tmq;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ReferenceDeserializer<T> implements Deserializer<T> {

    @Override
    public T deserialize(ResultSet data) throws InstantiationException, IllegalAccessException, SQLException {
        Class<T> clazz = getGenericType();
        T t = clazz.newInstance();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            if (field.getType().isAssignableFrom(String.class)) {
                field.set(t, data.getString(field.getName()));
            }
            if (field.getType().isAssignableFrom(Integer.class)) {
                field.set(t, data.getInt(field.getName()));
            }
            if (field.getType().isAssignableFrom(Short.class)) {
                field.set(t, data.getShort(field.getName()));
            }
            if (field.getType().isAssignableFrom(Byte.class)) {
                field.set(t, data.getByte(field.getName()));
            }
            if (field.getType().isAssignableFrom(Float.class)) {
                field.set(t, data.getFloat(field.getName()));
            }
            if (field.getType().isAssignableFrom(Double.class)) {
                field.set(t, data.getDouble(field.getName()));
            }
            if (field.getType().isAssignableFrom(Long.class)) {
                field.set(t, data.getLong(field.getName()));
            }
            if (field.getType().isAssignableFrom(Timestamp.class)) {
                field.set(t, data.getTimestamp(field.getName()));
            }
            if (field.getType().isAssignableFrom(Boolean.class)) {
                field.set(t, data.getBoolean(field.getName()));
            }
            if (field.getType().isAssignableFrom(Byte[].class)) {
                field.set(t, data.getBytes(field.getName()));
            }
        }
        return t;
    }

    public Class<T> getGenericType() {
        Type type = getClass().getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return (Class<T>) parameterizedType.getActualTypeArguments()[0];
        }
        throw new RuntimeException();
    }
}
