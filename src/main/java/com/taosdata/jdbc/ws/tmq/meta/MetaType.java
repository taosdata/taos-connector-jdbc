package com.taosdata.jdbc.ws.tmq.meta;

public enum MetaType {
    // 类型，create-创建表，drop-删除表，alter-修改表，delete-删除数据
    CREATE,
    DROP,
    ALTER,
    DELETE;
}
