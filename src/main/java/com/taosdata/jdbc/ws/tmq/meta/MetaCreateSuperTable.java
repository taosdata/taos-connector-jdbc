package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;

public class MetaCreateSuperTable extends Meta {
    private List<Column> columns;
    private List<Column> tags;

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getTags() {
        return tags;
    }

    public void setTags(List<Column> tags) {
        this.tags = tags;
    }
}