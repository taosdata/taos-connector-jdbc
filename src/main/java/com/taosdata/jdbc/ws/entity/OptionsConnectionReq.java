package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.utils.ReqId;

import java.util.Collections;
import java.util.List;

/**
 * options_connection request pojo, used to change connection options at runtime.
 */
public class OptionsConnectionReq extends Payload {

    // keep in sync with TSDB_OPTION_CONNECTION_TIMEZONE in taos.h
    public static final int OPTION_CONNECTION_TIMEZONE = 1;

    @JsonProperty("options")
    private List<Option> options;

    public OptionsConnectionReq() {
        this.setReqId(ReqId.getReqID());
    }

    public static OptionsConnectionReq ofTimezone(String timezone) {
        OptionsConnectionReq req = new OptionsConnectionReq();
        // a null value clears the option on the server side
        req.setOptions(Collections.singletonList(new Option(OPTION_CONNECTION_TIMEZONE, timezone)));
        return req;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    public static class Option {
        @JsonProperty("option")
        private int option;
        @JsonProperty("value")
        private String value;

        public Option() {
        }

        public Option(int option, String value) {
            this.option = option;
            this.value = value;
        }

        public int getOption() {
            return option;
        }

        public void setOption(int option) {
            this.option = option;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
