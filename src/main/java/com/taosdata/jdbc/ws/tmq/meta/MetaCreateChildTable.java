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

import java.util.List;
import java.util.Objects;

public class MetaCreateChildTable extends Meta {
  private String using;
  private int tagNum;
  private List<Tag> tags;
  private List<ChildColRef> refs;
  private List<ChildTableInfo> createList;

  public String getUsing() {
    return using;
  }

  public void setUsing(String using) {
    this.using = using;
  }

  public int getTagNum() {
    return tagNum;
  }

  public void setTagNum(int tagNum) {
    this.tagNum = tagNum;
  }

  public List<Tag> getTags() {
    return tags;
  }

  public void setTags(List<Tag> tags) {
    this.tags = tags;
  }

  public List<ChildColRef> getRefs() {
    return refs;
  }

  public void setRefs(List<ChildColRef> refs) {
    this.refs = refs;
  }

  public List<ChildTableInfo> getCreateList() {
    return createList;
  }

  public void setCreateList(List<ChildTableInfo> createList) {
    this.createList = createList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MetaCreateChildTable that = (MetaCreateChildTable) o;
    return tagNum == that.tagNum
        && Objects.equals(using, that.using)
        && Objects.equals(tags, that.tags)
        && Objects.equals(refs, that.refs)
        && Objects.equals(createList, that.createList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), using, tagNum, tags, refs, createList);
  }
}
