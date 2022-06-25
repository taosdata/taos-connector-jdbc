package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TaosGlobalConfig;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class ReferenceDeserializer<V> implements Deserializer<V> {
    private Param[] params;

    @Override
    public void configure(Map<?, ?> configs) {
        Object encodingValue = configs.get(TMQConstants.VALUE_DESERIALIZER_ENCODING);
        if (encodingValue instanceof String)
            TaosGlobalConfig.setCharset(((String) encodingValue).trim());
    }

    @Override
    public V deserialize(ResultSet data) throws InstantiationException, IllegalAccessException, IntrospectionException, SQLException, InvocationTargetException {
        Class<V> clazz = getGenericType();
        V t = clazz.newInstance();

        if (params == null) {
            List<Param> lists = new ArrayList<>();
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
                String name = property.getName();
                if ("class".equals(name))
                    continue;
                Method method = property.getWriteMethod();
                Param param = new Param();
                param.name = name;
                param.method = method;
                param.clazz = method.getParameterTypes()[0];
                lists.add(param);
            }
            params = lists.toArray(new Param[0]);
        }

        for (Param param : params) {
            if (param.clazz.isAssignableFrom(String.class)) {
                param.method.invoke(t, data.getString(param.name));
            } else if (param.clazz.isAssignableFrom(Integer.class)
                    || param.clazz.isAssignableFrom(int.class)) {
                param.method.invoke(t, data.getInt(param.name));
            } else if (param.clazz.isAssignableFrom(Short.class)
                    || param.clazz.isAssignableFrom(short.class)) {
                param.method.invoke(t, data.getShort(param.name));
            } else if (param.clazz.isAssignableFrom(Byte.class)
                    || param.clazz.isAssignableFrom(byte.class)) {
                param.method.invoke(t, data.getByte(param.name));
            } else if (param.clazz.isAssignableFrom(Character.class)
                    || param.clazz.isAssignableFrom(char.class)) {
                param.method.invoke(t, (char) data.getByte(param.name));
            } else if (param.clazz.isAssignableFrom(Float.class)
                    || param.clazz.isAssignableFrom(float.class)) {
                param.method.invoke(t, data.getFloat(param.name));
            } else if (param.clazz.isAssignableFrom(Double.class)
                    || param.clazz.isAssignableFrom(double.class)) {
                param.method.invoke(t, data.getDouble(param.name));
            } else if (param.clazz.isAssignableFrom(Long.class)
                    || param.clazz.isAssignableFrom(long.class)) {
                param.method.invoke(t, data.getLong(param.name));
            } else if (param.clazz.isAssignableFrom(Boolean.class)
                    || param.clazz.isAssignableFrom(boolean.class)) {
                param.method.invoke(t, data.getLong(param.name));
            } else if (param.clazz.isAssignableFrom(String.class)) {
                param.method.invoke(t, data.getString(param.name));
            } else if (param.clazz.isAssignableFrom(Timestamp.class)) {
                param.method.invoke(t, data.getTimestamp(param.name));
            } else if (param.clazz.isAssignableFrom(Byte[].class)
                    || param.clazz.isAssignableFrom(byte[].class)) {
                param.method.invoke(t, data.getBytes(param.name));
            }
        }
        return t;
    }

    private Class<V> getGenericType() {
        Type type = getClass().getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return (Class<V>) parameterizedType.getActualTypeArguments()[0];
        }
        throw new RuntimeException();
    }

    private static class Param {
        String name;
        Method method;
        Class<?> clazz;
    }
}
