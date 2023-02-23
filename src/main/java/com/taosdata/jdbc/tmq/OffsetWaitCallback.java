package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBError;

import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.TMQ_SUCCESS;

public class OffsetWaitCallback<V> {
    private final ConsumerRecords<V> cRecord;

    private final JNIConsumer<?> consumer;
    private final OffsetCommitCallback<V> callback;

    public OffsetWaitCallback(ConsumerRecords<V> cRecord, JNIConsumer<?> consumer, OffsetCommitCallback<V> callback) {
        this.cRecord = cRecord;
        this.consumer = consumer;
        this.callback = callback;
    }

    @SuppressWarnings("unused")
    public void commitCallbackHandler(int code) throws SQLException {
        if (TMQ_SUCCESS != code) {
            Exception exception = TSDBError.createSQLException(code, consumer.getErrMsg(code));

            callback.onComplete(cRecord, exception);
        } else {
            callback.onComplete(cRecord, null);
        }

        consumer.closeOffset(cRecord.getOffset());
        consumer.releaseResultSet(cRecord.getOffset());
    }
}
