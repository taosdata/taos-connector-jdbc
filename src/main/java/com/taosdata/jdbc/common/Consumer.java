package com.taosdata.jdbc.common;

import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.OffsetCommitCallback;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public interface Consumer<V> {

    void create(Properties properties) throws SQLException;

    void subscribe(Collection<String> topics) throws SQLException;

    void unsubscribe() throws SQLException;

    Set<String> subscription() throws SQLException;

    ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException;

    void commitSync() throws SQLException;

    void close() throws SQLException;

    // For compatibility with previous(jni) versions
    void commitCallbackHandler(int code, OffsetCommitCallback callback);

    void commitAsync(TaosConsumer<?> consumer);
}
