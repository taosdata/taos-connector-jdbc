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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class Column {
  private String name;
  private int type;
  private Integer length;

  @JsonProperty("isPrimarykey")
  private Boolean primarykey;

  private String encode;
  private String compress;
  private String level;
  private ColRef ref;

  public Column() {}

  public Column(
      String name,
      int type,
      Integer length,
      Boolean primarykey,
      String encode,
      String compress,
      String level) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.primarykey = primarykey;
    this.encode = encode;
    this.compress = compress;
    this.level = level;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public Boolean isPrimarykey() {
    return primarykey;
  }

  public void setPrimarykey(Boolean primarykey) {
    this.primarykey = primarykey;
  }

  public String getEncode() {
    return encode;
  }

  public void setEncode(String encode) {
    this.encode = encode;
  }

  public String getCompress() {
    return compress;
  }

  public void setCompress(String compress) {
    this.compress = compress;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public ColRef getRef() {
    return ref;
  }

  public void setRef(ColRef ref) {
    this.ref = ref;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column column = (Column) o;
    return type == column.type
        && Objects.equals(name, column.name)
        && Objects.equals(length, column.length)
        && Objects.equals(primarykey, column.primarykey)
        && Objects.equals(encode, column.encode)
        && Objects.equals(compress, column.compress)
        && Objects.equals(level, column.level)
        && Objects.equals(ref, column.ref);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, length, primarykey, encode, compress, level, ref);
  }
}
