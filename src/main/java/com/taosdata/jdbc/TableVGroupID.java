package com.taosdata.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class TableVGroupID {
    public TableVGroupID(Connection connection) throws SQLException {
        if (!(connection instanceof TSDBConnection)) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "only supported by TSDBConnection");
        }

        this.connection = (TSDBConnection) connection;
    }

    private TableVGroupID() {
    }


    /**
     * getTableVgID. get table's v-group id.
     *
     * @param db    db name
     * @param table table name
     * @return v-group id
     * @throws SQLException throws exception if fail.
     */
    public int getTableVgID(String db, String table) throws SQLException {
        return this.connection.getConnector().getTableVGroupID(db, table);
    }

    private TSDBConnection connection;

}
