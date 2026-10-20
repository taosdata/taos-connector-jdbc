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

import java.util.Objects;

public class TagAlter {
  private String colName;
  private String colValue;
  private boolean colValueNull;
  private String regexp;
  private String replacement;

  public TagAlter() {}

  public TagAlter(String colName, String colValue, boolean colValueNull) {
    this.colName = colName;
    this.colValue = colValue;
    this.colValueNull = colValueNull;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getColValue() {
    return colValue;
  }

  public void setColValue(String colValue) {
    this.colValue = colValue;
  }

  public boolean isColValueNull() {
    return colValueNull;
  }

  public void setColValueNull(boolean colValueNull) {
    this.colValueNull = colValueNull;
  }

  public String getRegexp() {
    return regexp;
  }

  public void setRegexp(String regexp) {
    this.regexp = regexp;
  }

  public String getReplacement() {
    return replacement;
  }

  public void setReplacement(String replacement) {
    this.replacement = replacement;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TagAlter tagAlter = (TagAlter) o;
    return colValueNull == tagAlter.colValueNull
        && Objects.equals(colName, tagAlter.colName)
        && Objects.equals(colValue, tagAlter.colValue)
        && Objects.equals(regexp, tagAlter.regexp)
        && Objects.equals(replacement, tagAlter.replacement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(colName, colValue, colValueNull, regexp, replacement);
  }
}
