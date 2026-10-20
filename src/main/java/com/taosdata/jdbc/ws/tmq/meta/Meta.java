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

public abstract class Meta {
  private MetaType type;
  private String tableName;
  private TableType tableType;

  @JsonProperty("isVirtual")
  private Boolean isVirtual;

  public MetaType getType() {
    return type;
  }

  public void setType(MetaType type) {
    this.type = type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableType getTableType() {
    return tableType;
  }

  public void setTableType(TableType tableType) {
    this.tableType = tableType;
  }

  public Boolean getIsVirtual() {
    return isVirtual;
  }

  public void setIsVirtual(Boolean isVirtual) {
    this.isVirtual = isVirtual;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Meta meta = (Meta) o;
    return type == meta.type
        && Objects.equals(tableName, meta.tableName)
        && tableType == meta.tableType
        && Objects.equals(isVirtual, meta.isVirtual);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, tableName, tableType, isVirtual);
  }
}
