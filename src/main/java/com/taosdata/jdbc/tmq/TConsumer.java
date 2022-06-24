package com.taosdata.jdbc.tmq;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;

public interface TConsumer<V> extends AutoCloseable {

    void subscribe(Collection<String> topics) throws SQLException;

    void unsubscribe() throws SQLException;

    Set<String> subscription() throws SQLException;

    ConsumerRecords<V> poll(Duration timeout) throws SQLException;

    void commitAsync();

    void commitAsync(OffsetCommitCallback callback);

    void commitSync() throws SQLException;

    @Override
    void close() throws SQLException;
}
