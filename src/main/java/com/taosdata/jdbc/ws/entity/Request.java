/*
 * Copyright (c) 2022 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * send to taosadapter
 */
public class Request implements Printable {
    private static final Logger log = LoggerFactory.getLogger(Request.class);

    @JsonProperty("action")
    private String action;
    @JsonProperty("args")
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
        ObjectMapper objectMapper = JsonUtil.getObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Request to string error", e);
            return "";
        }
    }

    @Override
    public String toPrintString() {
         if (args instanceof Printable) {
           return ((Printable) args).toPrintString();
        } else {
            return this.toString();
        }
    }
}