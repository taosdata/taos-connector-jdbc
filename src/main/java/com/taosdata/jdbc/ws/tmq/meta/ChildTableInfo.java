package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;

public class ChildTableInfo extends Meta {
    private String using;
    private int tagNum;
    private List<Column> tags;

    public String getUsing() {
        return using;
    }

    public void setUsing(String using) {
        this.using = using;
    }

    public int getTagNum() {
        return tagNum;
    }

    public void setTagNum(int tagNum) {
        this.tagNum = tagNum;
    }

    public List<Column> getTags() {
        return tags;
    }

    public void setTags(List<Column> tags) {
        this.tags = tags;
    }
}