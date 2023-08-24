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

    @Test
    public void test() {
        for (String sql : sqls) {
            Matcher matcher = TSWSPreparedStatement.INSERT_PATTERN.matcher(sql);
            String db = null;
            if (matcher.find()) {
                if (matcher.group(1).equals("?") && matcher.group(3) != null) {
                    String usingGroup = matcher.group(3);
                    if (usingGroup.contains(".")) {
                        String[] split = usingGroup.split("\\.");
                        db = split[0];
                    }
                } else {
                    String usingGroup = matcher.group(1);
                    if (usingGroup.contains(".")) {
                        String[] split = usingGroup.split("\\.");
                        db = split[0];
                    }
                }
                Assert.assertEquals("st", db);
            } else {
                throw new RuntimeException("not match sql: " + sql);
            }
        }
    }
}
