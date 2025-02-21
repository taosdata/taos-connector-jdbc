package com.taosdata.jdbc.ws.tmq.meta;

public enum MetaType {
    // Type: "CREATE" - Create tables, "DROP" - Drop tables, "ALTER" - Alter a table, "DELETE" - Delete data
    CREATE,
    DROP,
    ALTER,
    DELETE;
}
