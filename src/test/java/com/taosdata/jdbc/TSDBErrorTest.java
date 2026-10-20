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

package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.junit.Assert.*;

public class TSDBErrorTest {

    // ------------------------------ Test createRuntimeException(int, Throwable) ------------------------------
    @Test
    public void testCreateRuntimeException_WithThrowable() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_CONNECTION_CLOSED;
        Throwable cause = new RuntimeException("root cause");
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(errorCode);

        // When
        RuntimeException ex = TSDBError.createRuntimeException(errorCode, cause);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    // ------------------------------ Test createSQLWarning ------------------------------
    @Test
    public void testCreateSQLWarning() {
        // Given
        String warningMsg = "test warning message";

        // When
        SQLWarning warning = TSDBError.createSQLWarning(warningMsg);

        // Then
        assertEquals(warningMsg, warning.getMessage());
    }

    // ------------------------------ Test undeterminedExecutionError ------------------------------
    @Test
    public void testUndeterminedExecutionError() {
        // Given
        String expectedMsg = "Please either call clearBatch() to clean up context first, or use executeBatch() instead";

        // When
        SQLException ex = TSDBError.undeterminedExecutionError();

        // Then
        assertEquals(expectedMsg, ex.getMessage());
        assertNull(ex.getSQLState());
    }

    // ------------------------------ Test createIllegalArgumentException ------------------------------
    @Test
    public void testCreateIllegalArgumentException_ExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_CONNECTION_CLOSED;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(errorCode);

        // When
        IllegalArgumentException ex = TSDBError.createIllegalArgumentException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    @Test
    public void testCreateIllegalArgumentException_NonExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_UNKNOWN;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(TSDBErrorNumbers.ERROR_UNKNOWN);

        // When
        IllegalArgumentException ex = TSDBError.createIllegalArgumentException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    // ------------------------------ Test createRuntimeException(int) ------------------------------
    @Test
    public void testCreateRuntimeException_ExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_CONNECTION_CLOSED;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(errorCode);

        // When
        RuntimeException ex = TSDBError.createRuntimeException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    @Test
    public void testCreateRuntimeException_NonExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_UNKNOWN;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(TSDBErrorNumbers.ERROR_UNKNOWN);

        // When
        RuntimeException ex = TSDBError.createRuntimeException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    // ------------------------------ Test createRuntimeException(int, String) ------------------------------
    @Test
    public void testCreateRuntimeException_WithCustomMessage() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_CONNECTION_CLOSED;
        String customMsg = "custom runtime message";
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + customMsg;

        // When
        RuntimeException ex = TSDBError.createRuntimeException(errorCode, customMsg);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    @Test
    public void traditionalUnsupportedErrorCode_isRemovedFromTSDBErrorNumbers() {
        try {
            TSDBErrorNumbers.class.getDeclaredField("ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER");
            fail("ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER should be removed with line-mode cleanup");
        } catch (NoSuchFieldException expected) {
            assertTrue(true);
        }
    }

    @Test
    public void removedTraditionalErrorCode_fallsBackToUnknownMessage() {
        assertEquals(TSDBError.getErrorMessage(TSDBErrorNumbers.ERROR_UNKNOWN),
                TSDBError.getErrorMessage(0x2324));
    }

    // ------------------------------ Test createIllegalStateException ------------------------------
    @Test
    public void testCreateIllegalStateException_ExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_CONNECTION_CLOSED;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(errorCode);

        // When
        IllegalStateException ex = TSDBError.createIllegalStateException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }

    @Test
    public void testCreateIllegalStateException_NonExistingErrorCode() {
        // Given
        int errorCode = TSDBErrorNumbers.ERROR_UNKNOWN;
        String expectedMsg = "ERROR (0x" + Integer.toHexString(errorCode) + "): " + TSDBError.getErrorMessage(TSDBErrorNumbers.ERROR_UNKNOWN);

        // When
        IllegalStateException ex = TSDBError.createIllegalStateException(errorCode);

        // Then
        assertEquals(expectedMsg, ex.getMessage());
    }
}
