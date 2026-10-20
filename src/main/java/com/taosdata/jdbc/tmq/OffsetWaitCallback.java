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

package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBError;

import java.util.Map;

import static com.taosdata.jdbc.TSDBConstants.TMQ_SUCCESS;

public class OffsetWaitCallback<V> {
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    private final JNIConsumer<?> consumer;
    private final OffsetCommitCallback<V> callback;

    public OffsetWaitCallback(Map<TopicPartition, OffsetAndMetadata> offsets, JNIConsumer<?> consumer, OffsetCommitCallback<V> callback) {
        this.offsets = offsets;
        this.consumer = consumer;
        this.callback = callback;
    }

    @SuppressWarnings("unused")
    public void commitCallbackHandler(int code) {
        if (TMQ_SUCCESS != code) {
            Exception exception = TSDBError.createSQLException(code, consumer.getErrMsg(code));

            callback.onComplete(offsets, exception);
        } else {
            callback.onComplete(offsets, null);
        }

    }
}
