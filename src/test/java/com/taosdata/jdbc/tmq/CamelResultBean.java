package com.taosdata.jdbc.tmq;

import java.sql.Timestamp;

/**
 * @author gepengjun
 * @since 2022/10/28 10:46
 */
public class CamelResultBean {
    private Timestamp ts;
    private int fieldOne;
    private Float fieldTwo;
    private String fieldThree;
    private byte[] fieldFour;
    private Integer tagOne;


    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getFieldOne() {
        return fieldOne;
    }

    public void setFieldOne(int fieldOne) {
        this.fieldOne = fieldOne;
    }

    public Float getFieldTwo() {
        return fieldTwo;
    }

    public void setFieldTwo(Float fieldTwo) {
        this.fieldTwo = fieldTwo;
    }

    public String getFieldThree() {
        return fieldThree;
    }

    public void setFieldThree(String fieldThree) {
        this.fieldThree = fieldThree;
    }

    public byte[] getFieldFour() {
        return fieldFour;
    }

    public void setFieldFour(byte[] fieldFour) {
        this.fieldFour = fieldFour;
    }

    public Integer getTagOne() {
        return tagOne;
    }

    public void setTagOne(Integer tagOne) {
        this.tagOne = tagOne;
    }
}
