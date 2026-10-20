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

package com.taosdata.jdbc.ws.tmq.meta;

public enum AlterType {
  ADD_TAG(1),
  DROP_TAG(2),
  RENAME_TAG_NAME(3),
  SET_TAG(4),
  ADD_COLUMN(5),
  DROP_COLUMN(6),
  MODIFY_COLUMN_LENGTH(7),
  MODIFY_TAG_LENGTH(8),
  MODIFY_TABLE_OPTION(9),
  RENAME_COLUMN_NAME(10),
  ADD_TAG_INDEX(11),
  UPDATE_COLUMN_COMPRESS(13),
  ADD_COLUMN_WITH_COMPRESS(14),
  SET_MULTI_TAG(15),
  ALTER_COLUMN_REF(16),
  SET_REF_NULL(17),
  ADD_COLUMN_WITH_REF(18),
  ALTER_MULTI_TABLE_TAG(19),
  ALTER_STABLE_TAG_WITH_FILTER(20),
  ALTER_TAG_REF(21),
  ADD_BASE_ON(22),
  DROP_BASE_ON(23);
  private final int value;

  AlterType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
