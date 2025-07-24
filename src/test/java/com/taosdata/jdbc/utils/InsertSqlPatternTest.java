package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.common.BaseTest;

public class InsertSqlPatternTest extends BaseTest {
    String[] sqls = {
            "insert into st.pre values (?, ?)",
            "insert into st.pre (c1, c2) values (?, ?)",
            "insert into ?    using st.pre tags(?) values (?, ?)",
            "insert into ? using st.pre tags(?)values(?, ?)",
            "insert into ? using st.pre (t1, t2) tags(?,?) values (?, ?)",
            "insert into ? using st.pre (t1, t2)tags(?,?)values (?, ?)",
            "insert into ? using st.pre (t1, t2) tags(?,?) (c1, c2) values(?, ?)",
            "insert into st.sub_t using db.sup_t (t1, t2) tags(?,?) (c1, c2) values(?, ?)",
    };


}
