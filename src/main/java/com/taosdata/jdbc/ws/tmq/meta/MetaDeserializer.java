package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class MetaDeserializer extends StdDeserializer<Meta> {

    public MetaDeserializer() {
        this(null);
    }

    protected MetaDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Meta deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, IllegalArgumentException {
        ObjectCodec mapper = p.getCodec();
        JsonNode node = mapper.readTree(p);

        String type = node.get("type").asText();
        String tableType = node.get("tableType").asText();

        Meta result = getMetaBasedOnTableType(mapper, node, type, tableType);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported combination of 'type' and 'tableType' values: type=" + type + ", tableType=" + tableType);
        }
        return result;
    }

    private Meta getMetaBasedOnTableType(ObjectCodec mapper, JsonNode node, String type, String tableType) throws JsonProcessingException {
        if (TableType.SUPER.toString().equalsIgnoreCase(tableType)) {
            if (MetaType.CREATE.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaCreateSuperTable.class);
            } else if (MetaType.DROP.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropSuperTable.class);
            } else if (MetaType.ALTER.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropSuperTable.class);
            }
        } else if (TableType.NORMAL.toString().equalsIgnoreCase(tableType)) {
            if (MetaType.CREATE.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaCreateNormalTable.class);
            } else if (MetaType.DROP.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropNormalTable.class);
            } else if (MetaType.ALTER.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropNormalTable.class);
            }
        } else if (TableType.CHILD.toString().equalsIgnoreCase(tableType)) {
            if (MetaType.CREATE.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaCreateChildTable.class);
            } else if (MetaType.DROP.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropChildTable.class);
            } else if (MetaType.ALTER.toString().equalsIgnoreCase(type)) {
                return mapper.treeToValue(node, MetaDropChildTable.class);
            }
        }
        return null;
    }
}