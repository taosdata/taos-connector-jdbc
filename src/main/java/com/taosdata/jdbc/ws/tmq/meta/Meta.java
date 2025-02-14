package com.taosdata.jdbc.ws.tmq.meta;
public abstract class Meta {
    private MetaType type;
    private Sql sql;
    private String tableName;
    private TableType tableType;

    public MetaType getType() {
        return type;
    }

    public void setType(MetaType type) {
        this.type = type;
    }

    public Sql getSql() {
        return sql;
    }

    public void setSql(Sql sql) {
        this.sql = sql;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }
}
