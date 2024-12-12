package com.taosdata.jdbc.enums;

public enum FeildBindType {
    TAOS_FIELD_COL(1),
    TAOS_FIELD_TAG(2),
    TAOS_FIELD_QUERY(3),
    TAOS_FIELD_TBNAME(4);

    private final int value;

    FeildBindType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static FeildBindType fromValue(int value) {
        for (FeildBindType field : FeildBindType.values()) {
            if (field.value == value) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
