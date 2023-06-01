/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc.utils;

import java.util.regex.Pattern;

public class SqlSyntaxValidator {
    private static final Pattern USE_PATTERN = Pattern.compile("use\\s+(\\w+);?", Pattern.CASE_INSENSITIVE);

    private SqlSyntaxValidator() {
    }

    public static boolean isUseSql(String sql) {
        return sql.trim().toLowerCase().startsWith("use");
    }

    public static String getDatabaseName(String sql) {
        if (isUseSql(sql)) {
            sql = sql.split(";")[0].trim();
            return USE_PATTERN.matcher(sql).replaceAll("$1");
        }
        return null;
    }
}
