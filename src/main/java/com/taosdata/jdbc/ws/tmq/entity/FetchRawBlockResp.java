package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.TmqMessageType;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.DataTypeConverUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.DecimalUtil;
import com.taosdata.jdbc.ws.tmq.ConsumerAction;
import com.taosdata.jdbc.ws.entity.Response;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.taosdata.jdbc.TSDBErrorNumbers;

import static com.taosdata.jdbc.TSDBConstants.*;

public class FetchRawBlockResp extends Response {
    private ByteBuffer buffer;
    private long time;
    private int code;
    private String message;
    private short version;
    private long messageID;
    private short metaType;
    private int rawBlockLength;

    private RestfulResultSetMetaData metaData;
    private final List<RestfulResultSet.Field> fields = new ArrayList<>();
    private List<String> columnNames = new ArrayList<>();
    // data
    private List<List<Object>> resultData;
    private byte precision;
    private int rows = 0;
    private String tableName = "";

    public FetchRawBlockResp(ByteBuffer buffer) {
        this.setAction(ConsumerAction.FETCH_RAW_DATA.getAction());
        this.buffer = buffer;
    }

    public void init() {
        buffer.getLong(); // action id
        version = buffer.getShort();
        time = buffer.getLong();
        this.setReqId(buffer.getLong());
        code = buffer.getInt();
        int messageLen = buffer.getInt();
        byte[] msgBytes = new byte[messageLen];
        buffer.get(msgBytes);

        message = new String(msgBytes, StandardCharsets.UTF_8);
        messageID = buffer.getLong(); // message id
        metaType = buffer.getShort();
        rawBlockLength = buffer.getInt();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    public long getTime() {
        return time;
    }
    public int getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }
    public short getVersion() {
        return version;
    }


    private void skipHead() throws SQLException {
        byte version = buffer.get();
        if (version >= 100) {
            int skip = buffer.getInt();
            buffer.position(buffer.position() + skip);
        } else {
            int skip = getTypeSkip(version);
            buffer.position(buffer.position() + skip);

            version = buffer.get();
            skip = getTypeSkip(version);
            buffer.position(buffer.position() + skip);
        }
    }

    private int getTypeSkip(byte type) throws SQLException {
        switch (type) {
            case 1:
                return 8;
            case 2:
            case 3:
                return 16;
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FetchBlockRawResp getTypeSkip error, type: " + type);
        }
    }

    public void parseBlockInfos() throws SQLException {
        skipHead();
        int blockNum = buffer.getInt();
        int cols = 0;

        boolean withTableName = buffer.get() != 0;// skip withTableName
        buffer.get();// skip withSchema

        for (int i = 0; i < blockNum; i++) {
            int blockTotalLen = parseVariableByteInteger();
            buffer.position(buffer.position() + 17);
            precision = buffer.get();

            // only parse the first block's schema
            if (i == 0){
                int backupBlockPos = buffer.position();
                buffer.position(buffer.position() + blockTotalLen - 18);
                cols = parseZigzagVariableByteInteger();
                resultData = new ArrayList<>(cols);
                for (int j = 0; j < cols; j++) {
                    resultData.add(new ArrayList<>());
                }
                //version
                parseZigzagVariableByteInteger();
                for (int j = 0; j < cols; j++) {
                    RestfulResultSet.Field field = parseSchema();
                    fields.add(field);
                    columnNames.add(field.getName());
                }
                if(withTableName){
                    tableName = parseName();
                }
                buffer.position(backupBlockPos);
            }
            fetchBlockData();
            buffer.get(); // skip useless byte

            parseZigzagVariableByteInteger(); // skip ncols
            parseZigzagVariableByteInteger(); // skip version
            for (int j = 0; j < cols; j++) {
                skipSchema(withTableName);
            }
            if (withTableName){
                int tableNameLen = parseVariableByteInteger();
                buffer.position(buffer.position() + tableNameLen);
            }
        }
        if (!resultData.isEmpty()){
            // rows is the number of rows of the first column
            rows = resultData.get(0).size();
        }
    }
    public ConsumerRecords<TMQEnhMap> getEhnMapList(PollResp pollResp, ZoneId zoneId) throws SQLException {
        skipHead();
        int blockNum = buffer.getInt();
        int cols = 0;

        ConsumerRecords<TMQEnhMap> records = new ConsumerRecords<>();
        boolean withTableName = buffer.get() != 0;// skip withTableName
        buffer.get();// skip withSchema

        for (int i = 0; i < blockNum; i++) {
            int blockTotalLen = parseVariableByteInteger();
            buffer.position(buffer.position() + 17);
            precision = buffer.get();

            fields.clear();
            columnNames.clear();

            // parse the block's schema
            int backupBlockPos = buffer.position();
            buffer.position(buffer.position() + blockTotalLen - 18);
            cols = parseZigzagVariableByteInteger();
            resultData = new ArrayList<>(cols);
            for (int j = 0; j < cols; j++) {
                resultData.add(new ArrayList<>());
            }
            //version
            parseZigzagVariableByteInteger();
            for (int j = 0; j < cols; j++) {
                RestfulResultSet.Field field = parseSchema();
                fields.add(field);
                columnNames.add(field.getName());
            }
            if(withTableName){
                tableName = parseName();
            }
            buffer.position(backupBlockPos);

            fetchBlockData();
            buffer.get(); // skip useless byte

            parseZigzagVariableByteInteger(); // skip ncols
            parseZigzagVariableByteInteger(); // skip version
            for (int j = 0; j < cols; j++) {
                skipSchema(withTableName);
            }

            if (withTableName){
                int tableNameLen = parseVariableByteInteger();
                buffer.position(buffer.position() + tableNameLen);
            }

            // handle the data in this block
            int lineNum = resultData.get(0).size();
            for (int j = 0; j < lineNum; j++) {
                HashMap<String, Object> lineDataMap = new HashMap<>();
                for (int k = 0; k < cols; k++) {
                    if (fields.get(k).getTaosType() == TSDB_DATA_TYPE_TIMESTAMP){
                        Long o = (Long) DataTypeConverUtil.parseValue(TSDB_DATA_TYPE_TIMESTAMP, resultData.get(k).get(j));
                        Instant instant = DateTimeUtils.parseTimestampColumnData(o, precision);
                        Timestamp t = DateTimeUtils.getTimestamp(instant, zoneId);
                        lineDataMap.put(columnNames.get(k), t);
                        continue;
                    }
                    Object o = DataTypeConverUtil.parseValue(fields.get(k).getTaosType(), resultData.get(k).get(j));
                    lineDataMap.put(columnNames.get(k), o);
                }
                TMQEnhMap map = new TMQEnhMap(tableName, lineDataMap);
                ConsumerRecord<TMQEnhMap> r = new ConsumerRecord.Builder<TMQEnhMap>()
                        .topic(pollResp.getTopic())
                        .dbName(pollResp.getDatabase())
                        .vGroupId(pollResp.getVgroupId())
                        .offset(pollResp.getOffset())
                        .messageType(TmqMessageType.TMQ_RES_DATA)
                        .meta(null)
                        .value(map)
                        .build();
                TopicPartition tp = new TopicPartition(pollResp.getTopic(), pollResp.getVgroupId());
                records.put(tp, r);
            }
            resultData.clear();
        }

        return records;
    }

    private int parseVariableByteInteger() {
        int multiplier = 1;
        int value = 0;
        while (true) {
            int encodedByte = buffer.get();
            value += (encodedByte & 127) * multiplier;
            if ((encodedByte & 128) == 0) {
                break;
            }
            multiplier *= 128;
        }
        return value;
    }

    private int zigzagDecode(int n) {
        return (n >> 1) ^ (-(n & 1));
    }

    private int parseZigzagVariableByteInteger() {
        return zigzagDecode(parseVariableByteInteger());
    }
    private String parseName() {
        int nameLen = parseVariableByteInteger();
        byte[] name = new byte[nameLen - 1];
        buffer.get(name);
        buffer.position(buffer.position() + 1);
        return new String(name, StandardCharsets.UTF_8);
    }

    private RestfulResultSet.Field parseSchema() throws SQLException{
        int taosType = buffer.get();
        int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
        buffer.get(); // skip flag
        int bytes = parseZigzagVariableByteInteger();
        parseZigzagVariableByteInteger(); // skip colid
        String name = parseName();
        return new RestfulResultSet.Field(name, jdbcType, bytes, "", taosType, 0);
    }

    private void skipSchema(boolean withTableName){
        buffer.position(buffer.position() + 2);
        parseZigzagVariableByteInteger(); // skip bytes
        parseZigzagVariableByteInteger(); // skip colld
        int nameLen = parseVariableByteInteger();
        buffer.position(buffer.position() + nameLen);
    }

    private int getScaleFromRowBlock(ByteBuffer buffer, int pHeader, int colIndex) {
        // for decimal: |___bytes___|__empty__|___prec___|__scale___|
        int backupPos = buffer.position();
        buffer.position(pHeader);
        buffer.position(buffer.position() + colIndex * 5 + 1);
        int scale = buffer.getInt();
        buffer.position(backupPos);
        return scale & 0xFF;
    }

    private void fetchBlockData() throws SQLException {
        buffer.position(buffer.position() + 8);
        int numOfRows = buffer.getInt();
        int bitMapOffset = bitmapLen(numOfRows);
        int beforeColLenPos = buffer.position() + 16;
        int pHeader = buffer.position() + 16 + fields.size() * 5;
        buffer.position(pHeader);
        List<Integer> lengths = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            lengths.add(buffer.getInt());
        }
        pHeader = buffer.position();
        int length = 0;
        for (int i = 0; i < fields.size(); i++) {
            List<Object> col = resultData.get(i);
            int type = fields.get(i).getTaosType();
            int scale = 0;
            if (type == TSDB_DATA_TYPE_DECIMAL128 || type == TSDB_DATA_TYPE_DECIMAL64) {
                scale = getScaleFromRowBlock(buffer, beforeColLenPos, i);
            }
            switch (type) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        byte b = buffer.get();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(b);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        short s = buffer.getShort();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(s);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        int in = buffer.getInt();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(in);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        long l = buffer.getLong();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(l);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        float f = buffer.getFloat();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(f);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        double d = buffer.getDouble();
                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(d);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_JSON:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_GEOMETRY:{
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.getInt());
                    }
                    int start = buffer.position();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.position(start + offset.get(m));
                        int len = buffer.getShort() & 0xFFFF;
                        byte[] tmp = new byte[len];
                        buffer.get(tmp);
                        col.add(tmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_NCHAR: {
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.getInt());
                    }
                    int start = buffer.position();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.position(start + offset.get(m));
                        int len = (buffer.getShort() & 0xFFFF) / 4;
                        int[] tmp = new int[len];
                        for (int n = 0; n < len; n++) {
                            tmp[n] = buffer.getInt();
                        }
                        col.add(tmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DECIMAL128:
                case TSDB_DATA_TYPE_DECIMAL64:
                    int dataLen = type == TSDB_DATA_TYPE_DECIMAL128 ? 16 : 8;
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        byte[] tb = new byte[dataLen];
                        buffer.get(tb);

                        if (isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            BigDecimal t = DecimalUtil.getBigDecimal(tb, scale);
                            col.add(t);
                        }
                    }
                    break;
                default:
                    // unknown type, do nothing
                    col.add(null);
                    break;
            }
            pHeader += length + lengths.get(i);
            buffer.position(pHeader);
        }
    }

    private int bitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    private boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] & (1 << (7 - index))) == (1 << (7 - index));
    }


    public RestfulResultSetMetaData getMetaData() {
        return metaData;
    }

    public List<RestfulResultSet.Field> getFields() {
        return fields;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<Object>> getResultData() {
        return resultData;
    }

    public byte getPrecision() {
        return precision;
    }

    public int getRows() {
        return rows;
    }
    public String getTableName() {
        return tableName;
    }
}
