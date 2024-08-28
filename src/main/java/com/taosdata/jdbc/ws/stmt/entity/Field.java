package com.taosdata.jdbc.ws.stmt.entity;

public class Field {
    private String name;
    private byte field_type;
    private byte precision;
    private byte scale;
    private int bytes;

    // getters and setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte getFieldType() {
        return field_type;
    }

    public void setFieldType(byte field_type) {
        this.field_type = field_type;
    }

    public byte getPrecision() {
        return precision;
    }

    public void setPrecision(byte precision) {
        this.precision = precision;
    }

    public byte getScale() {
        return scale;
    }

    public void setScale(byte scale) {
        this.scale = scale;
    }

    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }
}