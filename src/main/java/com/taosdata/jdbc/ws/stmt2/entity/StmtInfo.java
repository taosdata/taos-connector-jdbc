/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.enums.FieldBindType;

import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;

public class StmtInfo {
    private long stmtId = 0;
    private int toBeBindTableNameIndex = -1;
    private int toBeBindTagCount;
    private int toBeBindColCount;
    private int precision;
    private List<Field> fields;
    private final String sql;

    protected final ArrayList<Byte> tagTypeList = new ArrayList<>();
    protected ArrayList<Byte> colTypeList = new ArrayList<>();
    protected boolean isInsert = false;
    public StmtInfo(String sql) {
        this.sql = sql;
    }
    public StmtInfo(Stmt2PrepareResp prepareResp, String sql) {
        this.stmtId = prepareResp.getStmtId();
        this.sql = sql;

        isInsert = prepareResp.isInsert();
        if (isInsert){
            fields = prepareResp.getFields();
            for (int i = 0; i < fields.size(); i++){
                Field field = fields.get(i);

                // timestamp precision is same in one database
                if (field.getFieldType() == TSDB_DATA_TYPE_TIMESTAMP){
                    precision = field.getPrecision();
                }

                if (field.getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                    toBeBindTableNameIndex = i;
                }
                if (field.getBindType() == FieldBindType.TAOS_FIELD_TAG.getValue()){
                    toBeBindTagCount++;
                    tagTypeList.add(field.getFieldType());
                }
                if (field.getBindType() == FieldBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount++;
                    colTypeList.add(field.getFieldType());
                }
            }
        } else if (prepareResp.getFieldsCount() > 0){
            toBeBindColCount = prepareResp.getFieldsCount();

            fields = new ArrayList<>();
            for (int i = 0; i < toBeBindColCount; i++){
                colTypeList.add((byte)-1);
                Field field = new Field();
                field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
                fields.add(field);
            }
        }
    }

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public int getToBeBindTableNameIndex() {
        return toBeBindTableNameIndex;
    }

    public void setToBeBindTableNameIndex(int toBeBindTableNameIndex) {
        this.toBeBindTableNameIndex = toBeBindTableNameIndex;
    }

    public int getToBeBindTagCount() {
        return toBeBindTagCount;
    }

    public void setToBeBindTagCount(int toBeBindTagCount) {
        this.toBeBindTagCount = toBeBindTagCount;
    }

    public int getToBeBindColCount() {
        return toBeBindColCount;
    }

    public void setToBeBindColCount(int toBeBindColCount) {
        this.toBeBindColCount = toBeBindColCount;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
    public List<Field> getFields() {
        return fields;
    }
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public String getSql() {
        return sql;
    }

    public ArrayList<Byte> getTagTypeList() {
        return tagTypeList;
    }

    public ArrayList<Byte> getColTypeList() {
        return colTypeList;
    }
    public void setColTypeList(ArrayList<Byte> colTypeList) {
        this.colTypeList = colTypeList;
    }

    public boolean isInsert() {
        return isInsert;
    }
}
