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

package com.taosdata.jdbc.ws.tmq.meta;

import com.taosdata.jdbc.utils.StringUtils;

import java.util.HashMap;
import java.util.Map;

public enum TableType {
    SUPER,
    CHILD,
    NORMAL;

    private static final Map<String, TableType> VALUE_MAP;

    static {
        VALUE_MAP = new HashMap<>(values().length);
        for (TableType type : values()) {
            VALUE_MAP.put(type.name().toUpperCase(), type);
        }
    }

    public static TableType fromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        TableType type = VALUE_MAP.get(value.toUpperCase());
        if (type == null) {
            throw new IllegalArgumentException("Invalid TableType value: " + value);
        }
        return type;
    }

    public boolean matches(String value) {
        return this == fromString(value);
    }
}
