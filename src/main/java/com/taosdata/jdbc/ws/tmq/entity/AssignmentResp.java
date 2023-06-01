package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.tmq.Assignment;
import com.taosdata.jdbc.ws.entity.Response;

public class AssignmentResp extends Response {
    private int code;
    private String message;
    private long timing;
    private Assignment[] assignment;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public Assignment[] getAssignment() {
        return assignment;
    }

    public void setAssignment(Assignment[] assignment) {
        this.assignment = assignment;
    }
}