/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

package com.taosdata.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

public abstract class AbstractParameterMetaData extends WrapperImpl implements ParameterMetaData {

    private final Object[] parameters;

    public AbstractParameterMetaData(Object[] parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameters == null ? 0 : parameters.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Byte)
            return true;
        if (parameters[param - 1] instanceof Short)
            return true;
        if (parameters[param - 1] instanceof Integer)
            return true;
        if (parameters[param - 1] instanceof Long)
            return true;
        if (parameters[param - 1] instanceof Float)
            return true;
        if (parameters[param - 1] instanceof Double)
            return true;

        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Boolean)
            return TSDBConstants.BOOLEAN_PRECISION;
        if (parameters[param - 1] instanceof Byte)
            return TSDBConstants.TINYINT_PRECISION;
        if (parameters[param - 1] instanceof Short)
            return TSDBConstants.SMALLINT_PRECISION;
        if (parameters[param - 1] instanceof Integer)
            return TSDBConstants.INT_PRECISION;
        if (parameters[param - 1] instanceof Long)
            return TSDBConstants.BIGINT_PRECISION;
        if (parameters[param - 1] instanceof Timestamp)
            return TSDBConstants.TIMESTAMP_MS_PRECISION;
        if (parameters[param - 1] instanceof Float)
            return TSDBConstants.FLOAT_PRECISION;
        if (parameters[param - 1] instanceof Double)
            return TSDBConstants.DOUBLE_PRECISION;
        if (parameters[param - 1] instanceof String)
            return ((String) parameters[param - 1]).length();
        if (parameters[param - 1] instanceof byte[])
            return ((byte[]) parameters[param - 1]).length;
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Float)
            return TSDBConstants.FLOAT_SCALE;
        if (parameters[param - 1] instanceof Double)
            return TSDBConstants.DOUBLE_SCALE;
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Timestamp)
            return Types.TIMESTAMP;
        if (parameters[param - 1] instanceof Instant)
            return Types.TIMESTAMP;
        if (parameters[param - 1] instanceof Byte)
            return Types.TINYINT;
        if (parameters[param - 1] instanceof Short)
            return Types.SMALLINT;
        if (parameters[param - 1] instanceof Integer)
            return Types.INTEGER;
        if (parameters[param - 1] instanceof Long)
            return Types.BIGINT;
        if (parameters[param - 1] instanceof Float)
            return Types.FLOAT;
        if (parameters[param - 1] instanceof Double)
            return Types.DOUBLE;
        if (parameters[param - 1] instanceof String)
            return Types.NCHAR;
        if (parameters[param - 1] instanceof byte[])
            return Types.BINARY;
        if (parameters[param - 1] instanceof Boolean)
            return Types.BOOLEAN;
        return Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Instant)
            return TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP);
        if (parameters[param - 1] instanceof Timestamp)
            return TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP);
        if (parameters[param - 1] instanceof Byte)
            return TSDBConstants.jdbcType2TaosTypeName(Types.TINYINT);
        if (parameters[param - 1] instanceof Short)
            return TSDBConstants.jdbcType2TaosTypeName(Types.SMALLINT);
        if (parameters[param - 1] instanceof Integer)
            return TSDBConstants.jdbcType2TaosTypeName(Types.INTEGER);
        if (parameters[param - 1] instanceof Long)
            return TSDBConstants.jdbcType2TaosTypeName(Types.BIGINT);
        if (parameters[param - 1] instanceof Float)
            return TSDBConstants.jdbcType2TaosTypeName(Types.FLOAT);
        if (parameters[param - 1] instanceof Double)
            return TSDBConstants.jdbcType2TaosTypeName(Types.DOUBLE);
        if (parameters[param - 1] instanceof String)
            return TSDBConstants.jdbcType2TaosTypeName(Types.NCHAR);
        if (parameters[param - 1] instanceof byte[])
            return TSDBConstants.jdbcType2TaosTypeName(Types.BINARY);
        if (parameters[param - 1] instanceof Boolean)
            return TSDBConstants.jdbcType2TaosTypeName(Types.BOOLEAN);

        return parameters[param - 1].getClass().getName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return parameters[param - 1].getClass().getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return ParameterMetaData.parameterModeUnknown;
    }
}
