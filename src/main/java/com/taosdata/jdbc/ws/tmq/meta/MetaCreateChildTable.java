package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;

public class MetaCreateChildTable extends Meta {
    private String using;
    private int tagNum;
    private List<Tag> tags;

    private List<ChildTableInfo> createList;

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

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
    public List<ChildTableInfo> getCreateList() {
        return createList;
    }

    public void setCreateList(List<ChildTableInfo> createList) {
        this.createList = createList;
    }

}