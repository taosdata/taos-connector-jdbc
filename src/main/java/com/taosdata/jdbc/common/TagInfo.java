package com.taosdata.jdbc.common;

import java.util.ArrayList;
import java.util.List;

public class TagInfo implements Comparable<TagInfo> {
    private Object data;
    // taos data type
    private final int type;
    private final int index;

    public TagInfo(int index, Object data, int type) {
        this.index = index;
        this.data = data;
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int compareTo(TagInfo c) {
        return this.index > c.index ? 1 : -1;
    }
}
