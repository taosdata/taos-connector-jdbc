/*
 * Copyright (c) 2020 TAOS Data, Inc.
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

package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.WrapperImpl;
import com.taosdata.jdbc.enums.DataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulResultSetMetaData extends WrapperImpl implements ResultSetMetaData {

    private String tableName = "";
    private final String database;
    private final List<RestfulResultSet.Field> fields;
    private final boolean varcharAsString;

    public RestfulResultSetMetaData(String database, List<RestfulResultSet.Field> fields, boolean varcharAsString) {
        this.database = database;
        this.fields = fields == null ? Collections.emptyList() : fields;
        this.varcharAsString = varcharAsString;
    }

    public RestfulResultSetMetaData(String database, List<RestfulResultSet.Field> fields, String tableName, boolean varcharAsString) {
        this.tableName = tableName;
        this.database = database;
        this.fields = fields == null ? Collections.emptyList() : fields;
        this.varcharAsString = varcharAsString;
    }

    public List<RestfulResultSet.Field> getFields() {
        return fields;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return fields.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        if (column == 1)
            return ResultSetMetaData.columnNoNulls;
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = this.fields.get(column - 1).type;
        switch (type) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return this.fields.get(column - 1).length;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return fields.get(column - 1).name;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return fields.get(column - 1).name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        int type = this.fields.get(column - 1).taosType;
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_VARBINARY:
            case TSDBConstants.TSDB_DATA_TYPE_GEOMETRY:
            case TSDBConstants.TSDB_DATA_TYPE_BLOB:
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
                return this.fields.get(column - 1).length;
            case TSDBConstants.TSDB_DATA_TYPE_DECIMAL64:
            case TSDBConstants.TSDB_DATA_TYPE_DECIMAL128:
                return this.fields.get(column - 1).precision;
            default:
                return DataType.convertTaosType2DataType(type).getSize();
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        int type = this.fields.get(column - 1).taosType;
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_DECIMAL64:
            case TSDBConstants.TSDB_DATA_TYPE_DECIMAL128:
                return this.fields.get(column - 1).scale;
            default:
                return 0;
        }
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.database;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return this.fields.get(column - 1).type;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int taosType = fields.get(column - 1).taosType;
        if (taosType == TSDBConstants.TSDB_DATA_TYPE_BINARY && varcharAsString){
            return "VARCHAR";
        }
        return DataType.convertTaosType2DataType(taosType).getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = this.fields.get(column - 1).taosType;
        if (type == TSDBConstants.TSDB_DATA_TYPE_BINARY && varcharAsString) {
            return String.class.getName();
        }
        return DataType.convertTaosType2DataType(type).getClassName();
    }

}
