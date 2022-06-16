package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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

    long resultSet;
    private TMQConnector connector;
    private Consumer<CallbackResult> callback;
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(10),
            r -> {
                Thread t = new Thread(r);
                t.setName("consumer-callback-" + t.getId());
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

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
        if (null != properties) {
            String url = properties.getProperty(TMQConstants.CONNECT_URL);
            if (!StringUtils.isEmpty(url)) {
                properties = StringUtils.parseUrl(url, properties);
            }
        }
        long config = connector.createConfig(properties, this);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    public void commitCallbackHandler(int code, long offset) {
        CallbackResult r = new CallbackResult(code, this, offset);
        executor.submit(() -> callback.accept(r));
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
            resultSet = connector.poll(timeout.toMillis());
            if (resultSet == 0) {
                return new EmptyResultSet();
            }
            int timestampPrecision = connector.getResultTimePrecision(resultSet);
            return new TMQResultSet(connector, resultSet, timestampPrecision);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync() {
        // currently offset is zero
        connector.asyncCommit(0, this);
    }


    @Override
    public void commitAsync(Consumer<CallbackResult> consumer) {
        // currently offset is zero
        this.callback = consumer;
        connector.asyncCommit(0, this);
    }

    @Override
    public void commitSync() throws SQLException {
        connector.syncCommit(0);
    }

    @Override
    public String getTopicName() {
        return connector.getTopicName(resultSet);
    }

    @Override
    public String getDatabaseName() {
        return connector.getDbName(resultSet);
    }

    @Override
    public int getVgroupId() {
        return connector.getVgroupId(resultSet);
    }

    @Override
    public String getTableName() {
        return connector.getTableName(resultSet);
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
            executor.shutdown();
            connector.closeConsumer();
        } finally {
            closed = true;
            release();
        }
    }
}
