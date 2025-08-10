package com.taosdata.jdbc.enums;

public enum FetchState {
    PAUSED(1),
    FETCHING(2),
    COMPLETED(3)
    ;
    private final long state;

    FetchState(int state) {
        this.state = state;
    }

    public long get() {
        return state;
    }
}
