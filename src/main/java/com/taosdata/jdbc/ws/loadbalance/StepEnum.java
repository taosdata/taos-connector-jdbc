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

package com.taosdata.jdbc.ws.loadbalance;

public enum StepEnum {
    CONNECT(0),
    CON_CMD(1),
    QUERY(2),
    FETCH(3),
    FETCH2(4),
    FREE_RESULT(5),
    CLOSE(6),
    FINISH(7),

    ;
    private final int step;

    StepEnum(int step) {
        this.step = step;
    }

    public int get() {
        return step;
    }

    public static String getNameByInt(int step) {
        for (StepEnum stepEnum : values()) {
            if (stepEnum.step == step) {
                return stepEnum.name(); // return the enum name as a string
            }
        }
        throw new IllegalArgumentException("No matching StepEnum for int value: " + step);
    }
}
