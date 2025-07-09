package com.taosdata.jdbc.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Timestamp;

public class Bean {

    private int t1;
    private long ts;
    private int c1;
    private long c2;


    private float c3;
    private double c4;
    private byte[] c5;
    private short c6;

    private byte c7;
    private boolean c8;
    private String c9;
    private byte[] c10;
    private byte[] c11;
    private BigDecimal c12;
    private Timestamp c13;
    private short c14;
    private int c15;
    private long c16;
    private BigInteger c17;
    private BigDecimal c18;
    private BigDecimal c19;
    private Blob c20;
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Bean{");
        sb.append("ts=").append(ts);
        sb.append(", t1=").append(t1);
        sb.append(", c1=").append(c1);
        sb.append(", c2=").append(c2);
        sb.append(", c3=").append(c3);
        sb.append(", c4=").append(c4);
        sb.append(", c5=").append(java.util.Arrays.toString(c5));
        sb.append(", c6=").append(c6);
        sb.append(", c7=").append(c7);
        sb.append(", c8=").append(c8);
        sb.append(", c9='").append(c9).append('\'');
        sb.append(", c10=").append(java.util.Arrays.toString(c10));
        sb.append(", c11=").append(java.util.Arrays.toString(c11));
        sb.append(", c12=").append(c12);
        sb.append(", c13=").append(c13);
        sb.append(", c14=").append(c14);
        sb.append(", c15=").append(c15);
        sb.append(", c16=").append(c16);
        sb.append(", c17=").append(c17);
        sb.append(", c18=").append(c18);
        sb.append(", c19=").append(c19);
        sb.append(", c20=").append(c20);
        sb.append('}');
        return sb.toString();
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public long getC2() {
        return c2;
    }

    public void setC2(long c2) {
        this.c2 = c2;
    }

    public float getC3() {
        return c3;
    }

    public void setC3(float c3) {
        this.c3 = c3;
    }

    public double getC4() {
        return c4;
    }

    public void setC4(double c4) {
        this.c4 = c4;
    }

    public byte[] getC5() {
        return c5;
    }

    public void setC5(byte[] c5) {
        this.c5 = c5;
    }

    public short getC6() {
        return c6;
    }

    public void setC6(short c6) {
        this.c6 = c6;
    }

    public byte getC7() {
        return c7;
    }

    public void setC7(byte c7) {
        this.c7 = c7;
    }

    public boolean isC8() {
        return c8;
    }

    public void setC8(boolean c8) {
        this.c8 = c8;
    }

    public String getC9() {
        return c9;
    }

    public void setC9(String c9) {
        this.c9 = c9;
    }

    public byte[] getC10() {
        return c10;
    }

    public void setC10(byte[] c10) {
        this.c10 = c10;
    }

    public byte[] getC11() {
        return c11;
    }

    public void setC11(byte[] c11) {
        this.c11 = c11;
    }
    public BigDecimal getC12() {
        return c12;
    }

    public void setC12(BigDecimal c12) {
        this.c12 = c12;
    }

    public Timestamp getC13() {
        return c13;
    }

    public void setC13(Timestamp c13) {
        this.c13 = c13;
    }


    public short getC14() {
        return c14;
    }

    public void setC14(short c14) {
        this.c14 = c14;
    }

    public int getC15() {
        return c15;
    }

    public void setC15(int c15) {
        this.c15 = c15;
    }

    public long getC16() {
        return c16;
    }

    public void setC16(long c16) {
        this.c16 = c16;
    }

    public BigInteger getC17() {
        return c17;
    }

    public void setC17(BigInteger c17) {
        this.c17 = c17;
    }

    public int getT1() {
        return t1;
    }

    public void setT1(int t1) {
        this.t1 = t1;
    }


    public BigDecimal getC18() {
        return c18;
    }

    public void setC18(BigDecimal c18) {
        this.c18 = c18;
    }

    public BigDecimal getC19() {
        return c19;
    }

    public void setC19(BigDecimal c19) {
        this.c19 = c19;
    }
    public Blob getC20() {
        return c20;
    }

    public void setC20(Blob c20) {
        this.c20 = c20;
    }
}
