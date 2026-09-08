package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaCreateSuperTable extends Meta {
    private List<Column> columns;
    private List<Column> tags;
    // VST inheritance (BASE ON): present only when this super table inherits from parents.
    private List<String> baseOn;
    private Integer ownColStart;
    private Integer ownTagStart;

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

    public List<String> getBaseOn() {
        return baseOn;
    }

    public void setBaseOn(List<String> baseOn) {
        this.baseOn = baseOn;
    }

    public Integer getOwnColStart() {
        return ownColStart;
    }

    public void setOwnColStart(Integer ownColStart) {
        this.ownColStart = ownColStart;
    }

    public Integer getOwnTagStart() {
        return ownTagStart;
    }

    public void setOwnTagStart(Integer ownTagStart) {
        this.ownTagStart = ownTagStart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaCreateSuperTable that = (MetaCreateSuperTable) o;
        return Objects.equals(columns, that.columns) && Objects.equals(tags, that.tags)
                && Objects.equals(baseOn, that.baseOn) && Objects.equals(ownColStart, that.ownColStart)
                && Objects.equals(ownTagStart, that.ownTagStart);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns, tags, baseOn, ownColStart, ownTagStart);
    }
}