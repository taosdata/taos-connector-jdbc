package com.taosdata.jdbc.ws.tmq.meta;

import com.taosdata.shaded.com.fasterxml.jackson.core.JsonParser;
import com.taosdata.shaded.com.fasterxml.jackson.databind.DeserializationContext;
import com.taosdata.shaded.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class TableTypeDeserializer extends JsonDeserializer<TableType> {
    @Override
    public TableType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String value = p.getText();
        return TableType.fromString(value);
    }
}