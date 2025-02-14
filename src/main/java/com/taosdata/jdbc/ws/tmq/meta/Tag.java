package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.databind.JsonNode;

public class Tag {
    private String name;
    private int type;
    private JsonNode value;

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

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }
}