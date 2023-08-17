package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;

public class InsertSqlPatternTest {
    @Test
    public void test() {
        String sql = "insert into st.pre values (?, ?)";
        String db  = "st";
        Matcher matcher = TSWSPreparedStatement.INSERT_PATTERN.matcher(sql);
        if (matcher.find()) {
            if (matcher.group(1).equals("?") && matcher.group(3) != null) {
                String usingGroup = matcher.group(3);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    System.out.println(split[0]);
                }
            } else {
                String usingGroup = matcher.group(1);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    Assert.assertEquals(db, split[0]);
                }
            }
        }else {
            throw new RuntimeException("not match");
        }
    }

    @Test
    public void test1() {
        String sql = "insert into ? using st.pre tags(?) values (?, ?)";
        String db  = "st";
        Matcher matcher = TSWSPreparedStatement.INSERT_PATTERN.matcher(sql);
        if (matcher.find()) {
            if (matcher.group(1).equals("?") && matcher.group(3) != null) {
                String usingGroup = matcher.group(3);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    Assert.assertEquals(db, split[0]);
                }
            } else {
                String usingGroup = matcher.group(1);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    Assert.assertEquals(db, split[0]);
                }
            }
        }else {
            throw new RuntimeException("not match");
        }
    }

    @Test
    public void test2() {
        String sql = "insert into ? using st.pre tags(?)values(?, ?)";
        String db  = "st";
        Matcher matcher = TSWSPreparedStatement.INSERT_PATTERN.matcher(sql);
        if (matcher.find()) {
            if (matcher.group(1).equals("?") && matcher.group(3) != null) {
                String usingGroup = matcher.group(3);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    Assert.assertEquals(db, split[0]);
                }
            } else {
                String usingGroup = matcher.group(1);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    Assert.assertEquals(db, split[0]);
                }
            }
        }else {
            throw new RuntimeException("not match");
        }
    }
}
