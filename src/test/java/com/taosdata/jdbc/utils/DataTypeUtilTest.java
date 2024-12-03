package com.taosdata.jdbc.utils;

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class DataTypeUtilTest {

    @Test
    public void testGetColumnClassName_NullType() {
        String className = DataTypeUtil.getColumnClassName(Types.NULL);
        assertEquals("java.lang.Object", className);
    }

    @Test
    public void testGetColumnClassName_TimestampType() {
        String className = DataTypeUtil.getColumnClassName(Types.TIMESTAMP);
        assertEquals("java.sql.Timestamp", className);
    }

    @Test
    public void testGetColumnClassName_NCharType() {
        String className = DataTypeUtil.getColumnClassName(Types.NCHAR);
        assertEquals("java.lang.String", className);
    }

    @Test
    public void testGetColumnClassName_DoubleType() {
        String className = DataTypeUtil.getColumnClassName(Types.DOUBLE);
        assertEquals("java.lang.Double", className);
    }

    @Test
    public void testGetColumnClassName_FloatType() {
        String className = DataTypeUtil.getColumnClassName(Types.FLOAT);
        assertEquals("java.lang.Float", className);
    }

    @Test
    public void testGetColumnClassName_BigIntType() {
        String className = DataTypeUtil.getColumnClassName(Types.BIGINT);
        assertEquals("java.lang.Long", className);
    }

    @Test
    public void testGetColumnClassName_IntegerType() {
        String className = DataTypeUtil.getColumnClassName(Types.INTEGER);
        assertEquals("java.lang.Integer", className);
    }

    @Test
    public void testGetColumnClassName_SmallIntType() {
        String className = DataTypeUtil.getColumnClassName(Types.SMALLINT);
        assertEquals("java.lang.Short", className);
    }

    @Test
    public void testGetColumnClassName_TinyIntType() {
        String className = DataTypeUtil.getColumnClassName(Types.TINYINT);
        assertEquals("java.lang.Byte", className);
    }

    @Test
    public void testGetColumnClassName_BooleanType() {
        String className = DataTypeUtil.getColumnClassName(Types.BOOLEAN);
        assertEquals("java.lang.Boolean", className);
    }

    @Test
    public void testGetColumnClassName_BinaryType() {
        String className = DataTypeUtil.getColumnClassName(Types.BINARY);
        assertEquals("[B", className);
    }

    @Test
    public void testGetColumnClassName_VarBinaryType() {
        String className = DataTypeUtil.getColumnClassName(Types.VARBINARY);
        assertEquals("[B", className);
    }

    @Test
    public void testGetColumnClassName_UnknownType() {
        String className = DataTypeUtil.getColumnClassName(999); // Unknown type
        assertEquals("", className); // Assuming it returns an empty string for unknown types
    }
}