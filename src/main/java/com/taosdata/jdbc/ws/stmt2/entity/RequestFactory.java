/*
 * Copyright (c) 2024 TAOS Data, Inc.
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

package com.taosdata.jdbc.ws.stmt2.entity;


import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Request;

/**
 * generate id for request
 */
public class RequestFactory {

    private RequestFactory() {
    }

    public static Request generateInit(long reqId, boolean singleStbInsert, boolean singleTableBindOnce) {
        InitReq initReq = new InitReq();
        initReq.setReqId(reqId);
        initReq.setSingleStbInsert(singleStbInsert);
        initReq.setSingleTableBindOnce(singleTableBindOnce);
        return new Request(Action.STMT2_INIT.getAction(), initReq);
    }

    public static Request generatePrepare(long stmtId, long reqId, String sql) {
        PrepareReq prepareReq = new PrepareReq();
        prepareReq.setReqId(reqId);
        prepareReq.setStmtId(stmtId);
        prepareReq.setSql(sql);
        return new Request(Action.STMT2_PREPARE.getAction(), prepareReq);
    }

    public static Request generateExec(long stmtId, long reqId) {
        ExecReq req = new ExecReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_EXEC.getAction(), req);
    }
    public static Request generateClose(long stmtId, long reqId) {
        CloseReq req = new CloseReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_CLOSE.getAction(), req);
    }
    public static Request generateUseResult(long stmtId, long reqId) {
        ResultReq req = new ResultReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_USE_RESULT.getAction(), req);
    }
}
