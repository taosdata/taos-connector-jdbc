/*
 * Copyright (c) 2023 TAOS Data, Inc.
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

package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.ws.entity.FetchReq;
import org.junit.Assert;
import org.junit.Test;

public class UInt64SerializerTest {

    @Test
    public void write() throws JsonProcessingException {
        FetchReq fetchReq = new FetchReq();
        fetchReq.setId(-1);
        fetchReq.setReqId(-1);

        // 使用 Jackson 将对象转换为 JSON 字符串
        String s = JsonUtil.getObjectWriter().writeValueAsString(fetchReq);
        Assert.assertTrue(s.contains(TSDBConstants.MAX_UNSIGNED_LONG));

        // 使用 Jackson 将 JSON 字符串解析为对象
        FetchReq fetchReq1 = JsonUtil.getObjectReader(FetchReq.class).readValue(s);
        Assert.assertEquals(-1, fetchReq1.getId());
    }
}