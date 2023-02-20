package com.taosdata.jdbc.enums;

public enum WSFunction {
    WS("ws"),
    TMQ("tmq"),
    STMT("stmt"),
    ;

    String function;

    WSFunction(String function) {
        this.function = function;
    }

    public String getFunction() {
        return function;
    }
}
