package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class JNIConsumer implements TAOSConsumer {

    private static final long NO_CURRENT_THREAD = -1L;
    // currentThread holds the threadId of the current thread accessing
    // used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private volatile boolean closed = false;


    private TMQConnector connector;
    private Consumer<CallbackResult> callback;

    public JNIConsumer(Properties properties) throws SQLException {
        new JNIConsumer(properties, null);
    }

    /**
     * Note: after creating a {@link JNIConsumer} you must always {@link #close()}
     * it to avoid resource leaks.
     */
    public JNIConsumer(Properties properties, Consumer<CallbackResult> callback) throws SQLException {
        this.callback = callback;
        if (null == callback) {
            this.callback = e -> {
            };
        }
        connector = new TMQConnector();
        properties = StringUtils.parseUrl(String.valueOf(properties.get(TMQConstants.CONNECT_URL)), properties);
        long config = connector.createConfig(properties);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    public void commitCallbackHandler(CallbackResult r) {
        callback.accept(r);
    }

    @Override
    public void subscribe(Collection<String> topics) throws SQLException {
        acquireAndEnsureOpen();
        long topicPointer = 0L;
        try {
            topicPointer = connector.createTopic(topics);
            connector.subscribe(topicPointer);
        } finally {
            if (topicPointer != TSDBConstants.JNI_NULL_POINTER) {
                connector.destroyTopic(topicPointer);
            }
            release();
        }
    }

    @Override
    public void unsubscribe() throws SQLException {
        acquireAndEnsureOpen();
        try {
            connector.unsubscribe();
        } finally {
            release();
        }
    }

    @Override
    public Set<String> subscription() throws SQLException {
        acquireAndEnsureOpen();
        try {
            return connector.subscription();
        } finally {
            release();
        }
    }

    @Override
    public ResultSet poll(Duration timeout) throws SQLException {
        acquireAndEnsureOpen();
        try {
            long resultSet = connector.poll(timeout.toMillis());
            if (resultSet == 0) {
                return new EmptyResultSet();
            }
            int timestampPrecision = connector.getResultTimePrecision(resultSet);
            return new TSDBResultSet(connector, resultSet, timestampPrecision);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync() {
        // currently offset is zero
        connector.asyncCommit(0);
    }

    @Override
    public void commitSync() {
        connector.syncCommit(0);
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
     * Instead of blocking
     * when the lock is not available, however, we just throw an exception (since
     * multi-threaded usage is not supported).
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
            connector.closeConsumer();
        } finally {
            closed = true;
            release();
        }
    }
}
