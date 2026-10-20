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

import com.taosdata.jdbc.ws.stmt2.entity.ResultResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import com.taosdata.jdbc.ws.tmq.entity.FetchRawBlockResp;

import java.util.HashMap;
import java.util.Map;

/**
 * request type
 */
public enum Action {
    VERSION("version", VersionResp.class),
    CONN("conn", ConnectResp.class),
    QUERY("query", QueryResp.class),
    //BINARY_QUERY("binary_query_with_result", QueryResp.class),
    BINARY_QUERY("binary_query", QueryResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_BLOCK("fetch_raw_block", FetchRawBlockResp.class),
    FETCH_BLOCK_NEW("fetch_block_new", FetchBlockNewResp.class),

    // free_result's class is meaningless
    FREE_RESULT("free_result", Response.class),

    // options
    OPTIONS_CONNECTION("options_connection", CommonResp.class),

    // stmt2
    STMT2_INIT("stmt2_init", Stmt2Resp.class),
    STMT2_PREPARE("stmt2_prepare", Stmt2PrepareResp.class),
    STMT2_BIND("stmt2_bind", Stmt2Resp.class),
    STMT2_EXEC("stmt2_exec", Stmt2ExecResp.class),
    STMT2_BIND_EXEC("stmt2_bind_exec", Stmt2ExecResp.class),
    // response means nothing
    STMT2_CLOSE("stmt2_close", Stmt2Resp.class),
    STMT2_USE_RESULT("stmt2_result", ResultResp.class),

    //schemaless
    INSERT("insert", CommonResp.class),
    ;
    private final String action;
    private final Class<? extends Response> clazz;

    Action(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getResponseClazz() {
        return clazz;
    }

    private static final Map<String, Action> actions = new HashMap<>();

    static {
        for (Action value : Action.values()) {
            actions.put(value.action, value);
        }
    }

    public static Action of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}
