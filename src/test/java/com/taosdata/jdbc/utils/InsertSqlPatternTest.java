package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;

public class InsertSqlPatternTest {
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
