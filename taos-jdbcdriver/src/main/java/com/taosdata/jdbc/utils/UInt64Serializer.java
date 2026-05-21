package com.taosdata.jdbc.utils;

import com.taosdata.shaded.com.fasterxml.jackson.core.JsonGenerator;
import com.taosdata.shaded.com.fasterxml.jackson.databind.JsonSerializer;
import com.taosdata.shaded.com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class UInt64Serializer extends JsonSerializer<Long> {
    @Override
    public void serialize(Long value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value < 0) {
            String encodedValue = Long.toUnsignedString(value);
            gen.writeString(encodedValue);
        } else {
            gen.writeNumber(value);
        }
    }
}
