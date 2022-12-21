package com.taosdata.jdbc.tmq;

import java.sql.Timestamp;

public class ResultBean {
    private Timestamp ts;
    private int c1;
    private Float c2;
    private String c3;
    private byte[] c4;
    private Integer t1;
    private boolean c5;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public Float getC2() {
        return c2;
    }

    public void setC2(Float c2) {
        this.c2 = c2;
    }

    public String getC3() {
        return c3;
    }

    public void setC3(String c3) {
        this.c3 = c3;
    }

    public byte[] getC4() {
        return c4;
    }

    public void setC4(byte[] c4) {
        this.c4 = c4;
    }

    public Integer getT1() {
        return t1;
    }

    public void setT1(Integer t1) {
        this.t1 = t1;
    }

    public boolean isC5() {
        return c5;
    }

    public void setC5(boolean c5) {
        this.c5 = c5;
    }
}
