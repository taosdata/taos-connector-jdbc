package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.ws.entity.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractWSResultSet extends AbstractResultSet {
    protected final Statement statement;
    protected final Transport transport;
    protected final long queryId;
    protected final long reqId;

    protected volatile boolean isClosed;
    // meta
    protected final ResultSetMetaData metaData;
    protected final List<RestfulResultSet.Field> fields = new ArrayList<>();
    protected final List<String> columnNames;
    protected List<Integer> fieldLength;
    // data
    protected List<List<Object>> result = new ArrayList<>();

    protected int numOfRows = 0;
    protected int rowIndex = 0;
    private boolean isCompleted;

    protected AbstractWSResultSet(Statement statement, Transport transport,
                               QueryResp response, String database) throws SQLException {
        this.statement = statement;
        this.transport = transport;
        this.queryId = response.getId();
        this.reqId = response.getReqId();
        columnNames = Arrays.asList(response.getFieldsNames());
        for (int i = 0; i < response.getFieldsCount(); i++) {
            String colName = response.getFieldsNames()[i];
            int taosType = response.getFieldsTypes()[i];
            int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
            int length = response.getFieldsLengths()[i];
            fields.add(new RestfulResultSet.Field(colName, jdbcType, length, "", taosType));
        }
        this.metaData = new RestfulResultSetMetaData(database, fields);
        this.timestampPrecision = response.getPrecision();
    }

    private boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        }

        if (this.forward()) {
            return true;
        }

        Request request = RequestFactory.generateFetch(queryId, reqId);
        FetchResp fetchResp = (FetchResp)transport.send(request);
        if (Code.SUCCESS.getCode() != fetchResp.getCode()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, fetchResp.getMessage());
        }
        this.reset();
        if (fetchResp.isCompleted() || fetchResp.getRows() == 0) {
            this.isCompleted = true;
            return false;
        }
        fieldLength = Arrays.asList(fetchResp.getLengths());
        this.numOfRows = fetchResp.getRows();
        this.result = fetchJsonData();
        return true;
    }

    public abstract List<List<Object>> fetchJsonData() throws SQLException;

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            if (!this.isClosed) {
                this.isClosed = true;
                if (result != null && !result.isEmpty() && !isCompleted) {
                    FetchReq closeReq = new FetchReq();
                    closeReq.setReqId(queryId);
                    closeReq.setId(queryId);
                    transport.sendWithoutRep(new Request(Action.FREE_RESULT.getAction(), closeReq));
                }
            }
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }
}
