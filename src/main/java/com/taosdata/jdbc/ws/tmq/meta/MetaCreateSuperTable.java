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

public class MetaCreateSuperTable extends Meta {
    private List<Column> columns;
    private List<Column> tags;
    // VST inheritance (BASE ON): present only when this super table inherits from parents.
    private List<String> baseOn;
    private Integer ownColStart;
    private Integer ownTagStart;

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getTags() {
        return tags;
    }

    public void setTags(List<Column> tags) {
        this.tags = tags;
    }

    public List<String> getBaseOn() {
        return baseOn;
    }

    public void setBaseOn(List<String> baseOn) {
        this.baseOn = baseOn;
    }

    public Integer getOwnColStart() {
        return ownColStart;
    }

    public void setOwnColStart(Integer ownColStart) {
        this.ownColStart = ownColStart;
    }

    public Integer getOwnTagStart() {
        return ownTagStart;
    }

    public void setOwnTagStart(Integer ownTagStart) {
        this.ownTagStart = ownTagStart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaCreateSuperTable that = (MetaCreateSuperTable) o;
        return Objects.equals(columns, that.columns) && Objects.equals(tags, that.tags)
                && Objects.equals(baseOn, that.baseOn) && Objects.equals(ownColStart, that.ownColStart)
                && Objects.equals(ownTagStart, that.ownTagStart);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns, tags, baseOn, ownColStart, ownTagStart);
    }
}