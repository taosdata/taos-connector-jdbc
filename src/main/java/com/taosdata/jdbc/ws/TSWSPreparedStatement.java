package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TaosPrepareStatement;

import java.sql.SQLException;

/**
 * Public contract type for all WebSocket prepared statements.
 *
 * <p>Historically this was the concrete class returned for every WebSocket
 * prepared statement. Since 3.9.0 the driver routes insert statements to
 * column-based implementations, so this type is kept as an interface to keep
 * {@code unwrap(TSWSPreparedStatement.class)} and {@code instanceof} working
 * across all WebSocket prepared statement implementations.
 */
public interface TSWSPreparedStatement extends TaosPrepareStatement {

    /**
     * Closes the column-data batch. Equivalent to {@link #close()}; declared here
     * for source compatibility with code that called it on TSWSPreparedStatement-typed
     * references when it was a concrete class.
     */
    default void columnDataCloseBatch() throws SQLException {
        close();
    }
}
