package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
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

        String tableType = null;
        if (null != node.get("tableType")){
            tableType = node.get("tableType").asText();
        }

        return getMetaBasedOnTableType(mapper, node, type, tableType);
    }

    private Meta getMetaBasedOnTableType(ObjectCodec mapper, JsonNode node, String type, String tableType) throws JsonProcessingException {
        if (MetaType.CREATE.toString().equalsIgnoreCase(type)) {
            if (TableType.SUPER.toString().equalsIgnoreCase(tableType)) {
                return mapper.treeToValue(node, MetaCreateSuperTable.class);
            } else if (TableType.NORMAL.toString().equalsIgnoreCase(tableType)) {
                return mapper.treeToValue(node, MetaCreateNormalTable.class);
            } else if (TableType.CHILD.toString().equalsIgnoreCase(tableType)) {
                return mapper.treeToValue(node, MetaCreateChildTable.class);
            }
        }

        if (MetaType.DROP.toString().equalsIgnoreCase(type)) {
            if (TableType.SUPER.toString().equalsIgnoreCase(tableType)) {
                return mapper.treeToValue(node, MetaDropSuperTable.class);
            } else {
                return mapper.treeToValue(node, MetaDropTable.class);
            }
        }

        if (MetaType.ALTER.toString().equalsIgnoreCase(type)) {
            return mapper.treeToValue(node, MetaAlterTable.class);
        }
        throw new IllegalArgumentException("Unsupported combination of 'type' and 'tableType' values: type=" + type + ", tableType=" + tableType);
    }
}