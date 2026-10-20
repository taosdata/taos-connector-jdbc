/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.Objects;

public class ChildColRef {
  private String colName;
  private String refDbName;
  private String refTableName;
  private String refColName;

  public ChildColRef() {}

  public ChildColRef(String colName, String refDbName, String refTableName, String refColName) {
    this.colName = colName;
    this.refDbName = refDbName;
    this.refTableName = refTableName;
    this.refColName = refColName;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getRefDbName() {
    return refDbName;
  }

  public void setRefDbName(String refDbName) {
    this.refDbName = refDbName;
  }

  public String getRefTableName() {
    return refTableName;
  }

  @JsonAlias("refTbName")
  public void setRefTableName(String refTableName) {
    this.refTableName = refTableName;
  }

  public String getRefColName() {
    return refColName;
  }

  public void setRefColName(String refColName) {
    this.refColName = refColName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChildColRef that = (ChildColRef) o;
    return Objects.equals(colName, that.colName)
        && Objects.equals(refDbName, that.refDbName)
        && Objects.equals(refTableName, that.refTableName)
        && Objects.equals(refColName, that.refColName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(colName, refDbName, refTableName, refColName);
  }

  @Override
  public String toString() {
    return "ChildColRef{"
        + "colName='" + colName + '\''
        + ", refDbName='" + refDbName + '\''
        + ", refTableName='" + refTableName + '\''
        + ", refColName='" + refColName + '\''
        + '}';
  }
}
