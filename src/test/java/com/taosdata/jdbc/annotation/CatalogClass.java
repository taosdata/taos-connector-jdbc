/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

package com.taosdata.jdbc.annotation;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class
 */
public class CatalogClass {

    private String name;
    private String alias;
    private String author;
    private String version;
    private List<CatalogMethod> methods = new ArrayList<>();
    private int total;
    private int failure;

    public void setTotal(int total) {
        this.total = total;
    }

    public void setFailure(int failure) {
        this.failure = failure;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setMethods(List<CatalogMethod> methods) {
        this.methods = methods;
    }

    @Override
    public String toString() {
        if (methods.size() < 1)
            return "";
        StringBuilder sb = new StringBuilder();
        sb.append("ClassName: ").append(name);
        String msg = trim(alias);
        if (null != msg) {
            sb.append("\tAlias:").append(alias);
            sb.append("\tTotal:").append(total)
                    .append("\tFailure:").append(failure).append("\n");
        }
        for (CatalogMethod method : methods) {
            sb.append("\t").append(method.getName());
            sb.append("\t").append(method.isSuccess());
            sb.append("\t").append(method.getMessage());
            String mAuthor = trim(method.getAuthor());
            if (null == mAuthor) {
                sb.append("\t").append(author);
            } else {
                sb.append("\t").append(method.getAuthor());
            }
            String mVersion = trim(method.getVersion());
            if (null == mVersion) {
                sb.append("\t").append(version);
            } else {
                sb.append("\t").append(mVersion);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String trim(String s) {
        if (null == s || s.trim().equals("")) {
            return null;
        } else {
            return s.trim();
        }
    }
}
