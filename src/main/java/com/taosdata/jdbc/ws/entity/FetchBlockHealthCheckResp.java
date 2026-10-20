/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

public class FetchBlockHealthCheckResp extends FetchBlockNewResp {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FetchBlockHealthCheckResp.class);

    public boolean isClusterAlive() {
        return isClusterAlive;
    }

    private boolean isClusterAlive;
    public FetchBlockHealthCheckResp(ByteBuf buffer) {
        super(buffer);
        super.init();

        if (getCode() == Code.SUCCESS.getCode() && !isCompleted()){
            try {
                int columns = 1;
                if (buffer != null) {
                    buffer.readIntLE(); // buffer length
                    int pHeader = buffer.readerIndex() + 28 + columns * 5;
                    buffer.skipBytes(8);
                    int numOfRows = buffer.readIntLE();

                    buffer.readerIndex(pHeader);
                    int bitMapOffset = bitmapLen(numOfRows);

                    List<Integer> lengths = new ArrayList<>(columns);
                    for (int i = 0; i < columns; i++) {
                        lengths.add(buffer.readIntLE());
                    }
                    pHeader = buffer.readerIndex();
                    int length = 0;
                    for (int i = 0; i < columns; i++) {
                        length = bitMapOffset;
                        byte[] tmp = new byte[bitMapOffset];
                        buffer.readBytes(tmp);
                        for (int j = 0; j < numOfRows; j++) {
                            int in = buffer.readIntLE();
                            isClusterAlive = in > 0;
                        }
                        pHeader += length + lengths.get(i);
                        buffer.readerIndex(pHeader);
                    }
                }
            } catch (Exception e){
                log.error("error on parse health check response", e);
            } finally {
                ReferenceCountUtil.safeRelease(buffer);
            }
        }
        else {
            isClusterAlive = false;
        }

    }

    public static FetchBlockHealthCheckResp getFailedResp() {
        FetchBlockHealthCheckResp resp = new FetchBlockHealthCheckResp(null);
        resp.isClusterAlive = false;
        return resp;
    }
    private int bitmapLen(int n) {
        return (n + 0x7) >> 3;
    }
}
