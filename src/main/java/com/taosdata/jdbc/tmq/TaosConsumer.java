package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerManager;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TaosConsumer<V> implements AutoCloseable {

    private static final long NO_CURRENT_THREAD = -1L;
    // currentThread holds the threadId of the current thread accessing
    // used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private volatile boolean closed = false;

    private final Consumer<V> consumer;
    private final Deserializer<V> deserializer;

    /**
     * Note: after creating a {@link TaosConsumer} you must always {@link #close()}
     * it to avoid resource leaks.
     */
    @SuppressWarnings("unchecked")
    public TaosConsumer(Properties properties) throws SQLException {
        if (null == properties)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_NULL, "consumer properties must not be null!");

        String servers = properties.getProperty(TMQConstants.BOOTSTRAP_SERVERS);
        if (!StringUtils.isEmpty(servers)) {
            Arrays.stream(servers.split(",")).filter(s -> !StringUtils.isEmpty(s))
                    .findFirst().ifPresent(s -> {
                        String[] host = s.split(":");
                        properties.setProperty(TMQConstants.CONNECT_IP, host[0]);
                        if (host.length > 1) {
                            properties.setProperty(TMQConstants.CONNECT_PORT, host[1]);
                        }
                    });
        }

        String s = properties.getProperty(TMQConstants.VALUE_DESERIALIZER);
        if (!StringUtils.isEmpty(s)) {
            deserializer = (Deserializer<V>) Utils.newInstance(Utils.parseClassType(s));
        } else {
            deserializer = (Deserializer<V>) new MapDeserializer();
        }

        deserializer.configure(properties);
        String type = properties.getProperty(TMQConstants.CONNECT_TYPE);
        consumer = (Consumer<V>) ConsumerManager.getConsumer(type);
        consumer.create(properties);
    }

    public void subscribe(Collection<String> topics) throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.subscribe(topics);
        } finally {
            release();
        }
    }

    public void unsubscribe() throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.unsubscribe();
        } finally {
            release();
        }
    }

    public Set<String> subscription() throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.subscription();
        } finally {
            release();
        }
    }

    public ConsumerRecords<V> poll(Duration timeout) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.poll(timeout, deserializer);
        } finally {
            release();
        }
    }

    /**
     * jni consumer will call back whit error code when commit async
     *
     * @param code error code
     */
    @Deprecated
    public void commitCallbackHandler(int code) {
    }

    @SuppressWarnings("unused")
    public void commitAsync() {
        consumer.commitAsync((r, e) -> {
        });
    }

    @SuppressWarnings("unused")
    public void commitAsync(OffsetCommitCallback<V> callback) {
        consumer.commitAsync(callback);
    }

    @SuppressWarnings("unused")
    public void commitSync() throws SQLException {
        consumer.commitSync();
    }

    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw TSDBError.createIllegalArgumentException(TSDBErrorNumbers.ERROR_TMQ_SEEK_OFFSET);

        acquireAndEnsureOpen();
        try {
            consumer.seek(partition, offset);
        } finally {
            release();
        }
    }

    public Map<TopicPartition, Long> endOffsets(String topic) {
        acquireAndEnsureOpen();
        try {
            return consumer.endOffsets(topic);
        } finally {
            release();
        }
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access.
     * Instead of blocking when the lock is not available, however,
     * we just throw an exception (since multi-threaded usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("Consumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    @Override
    public void close() throws SQLException {
        acquire();
        try {
            consumer.close();
        } finally {
            closed = true;
            release();
        }
    }
}
