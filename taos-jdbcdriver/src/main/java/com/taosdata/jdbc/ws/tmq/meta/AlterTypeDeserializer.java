package com.taosdata.jdbc.ws.tmq.meta;

import com.taosdata.shaded.com.fasterxml.jackson.core.JsonParser;
import com.taosdata.shaded.com.fasterxml.jackson.databind.DeserializationContext;
import com.taosdata.shaded.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class AlterTypeDeserializer extends JsonDeserializer<AlterType> {
    @Override
    public AlterType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        int value = p.getIntValue();
        for (AlterType type : AlterType.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        throw new IOException("Invalid alter type: " + value);
    }
}