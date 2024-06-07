package com.taosdata.jdbc.ws.tmq.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.taosdata.jdbc.utils.UInt64Codec;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class FetchResp extends CommonResp {

    @JSONField(name = "message_id", deserializeUsing = UInt64Codec.class)
    private long messageId;
    private boolean completed;

    @JSONField(name = "table_name")
    private String tableName;

    private int rows;

    @JSONField(name = "fields_count")
    private int fieldsCount;

    @JSONField(name = "fields_names")
    private String[] fieldsNames;

    @JSONField(name = "fields_types")
    private int[] fieldsTypes;

    @JSONField(name = "fields_lengths")
    private long[] fieldsLengths;

    private int precision;


    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getFieldsCount() {
        return fieldsCount;
    }

    public void setFieldsCount(int fieldsCount) {
        this.fieldsCount = fieldsCount;
    }

    public String[] getFieldsNames() {
        return fieldsNames;
    }

    public void setFieldsNames(String[] fieldsNames) {
        this.fieldsNames = fieldsNames;
    }

    public int[] getFieldsTypes() {
        return fieldsTypes;
    }

    public void setFieldsTypes(int[] fieldsTypes) {
        this.fieldsTypes = fieldsTypes;
    }

    public long[] getFieldsLengths() {
        return fieldsLengths;
    }

    public void setFieldsLengths(long[] fieldsLengths) {
        this.fieldsLengths = fieldsLengths;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
}
