package com.taosdata.jdbc.enums;

import com.taosdata.jdbc.utils.StringUtils;

public enum DriverType {
    JNI("jdbc:TAOS://"),
    REST("jdbc:TAOS-RS://"),
    UNKNOWN(""),
    ;
    private final String prefix;

    DriverType(String prefix) {
        this.prefix = prefix;
    }

    public static DriverType getType(String url) {
        if (StringUtils.isEmpty(url))
            return UNKNOWN;

        for (DriverType type : DriverType.values()) {
            if (url.trim().startsWith(type.prefix)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
