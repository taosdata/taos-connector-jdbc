package com.taosdata.jdbc.ws.tmq.meta;

public class Column {
    private String name;
    private int type;
    private int length;
    private boolean isPrimaryKey;
    public Column(String name, int type, int length, boolean isPrimaryKey) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }
}