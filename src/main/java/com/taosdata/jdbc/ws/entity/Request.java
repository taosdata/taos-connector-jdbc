package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * send to taosadapter
 */
public class Request {
    private final Logger log = LoggerFactory.getLogger(Request.class);

    private String action;
    private Payload args;

    public Request(String action, Payload args) {
        this.action = action;
        this.args = args;
    }

    public String getAction() {
        return action;
    }

    public Long id(){
        return args.getReqId();
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Payload getArgs() {
        return args;
    }

    public void setArgs(Payload args) {
        this.args = args;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Request to string error", e);
            return null;
        }
    }
}