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

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.File;
import java.io.FileWriter;
import java.util.LinkedList;

public class CatalogListener extends RunListener {
    public static final String CATALOG_FILE = "target/TestCaseCatalog.txt";
    CatalogClass catalogClass = null;
    private final LinkedList<CatalogMethod> methods = new LinkedList<>();

    @Override
    public void testRunStarted(Description description) throws Exception {
        catalogClass = new CatalogClass();
        TestTarget target = description.getAnnotation(TestTarget.class);
        if (target != null) {
            catalogClass.setAlias(target.alias());
            catalogClass.setAuthor(target.author());
            catalogClass.setVersion(target.version());
        }
        catalogClass.setName(getClassName(description.getClassName()));
    }

    private String getClassName(String name) {
        if (null == name || name.trim().equals("")) {
            return null;
        }
        name = name.trim();
        int pos = name.lastIndexOf(".");
        if (-1 == pos) {
            return name;
        }
        return name.substring(pos + 1);
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        catalogClass.setMethods(methods);
        catalogClass.setTotal(result.getRunCount());
        catalogClass.setFailure(result.getFailureCount());
        File file = new File(CATALOG_FILE);
        if (!file.exists()) {
            synchronized (CatalogListener.class) {
                if (!file.exists()) {
                    file.createNewFile();
                    try (FileWriter writer = new FileWriter(file, true)) {
                        writer.write("\tName\tPass\tMessage\tAuthor\tVersion\n");
                        writer.write(catalogClass.toString());
                    }
                }
            }
        } else {
            try (FileWriter writer = new FileWriter(file, true)) {
                writer.write(catalogClass.toString());
            }
        }
    }

    @Override
    public void testStarted(Description description) throws Exception {
    }

    @Override
    public void testFinished(Description description) throws Exception {
        com.taosdata.jdbc.annotation.Description annotation
                = description.getAnnotation(com.taosdata.jdbc.annotation.Description.class);
        if (annotation != null) {
            CatalogMethod method = new CatalogMethod();
            method.setMessage(annotation.value());
            method.setAuthor(annotation.author());
            method.setVersion(annotation.version());
            method.setSuccess(true);
            method.setName(description.getMethodName());
            methods.addLast(method);
        }
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        com.taosdata.jdbc.annotation.Description annotation
                = failure.getDescription().getAnnotation(com.taosdata.jdbc.annotation.Description.class);
        if (null == annotation)
            return;
        CatalogMethod method = new CatalogMethod();
        method.setMessage(annotation.value());
        method.setAuthor(annotation.author());
        method.setVersion(annotation.version());
        method.setSuccess(false);
        method.setName(failure.getDescription().getMethodName());
        methods.addFirst(method);
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
    }

    @Override
    public void testIgnored(Description description) throws Exception {
        super.testIgnored(description);
    }
}