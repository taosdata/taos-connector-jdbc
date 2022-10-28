package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.utils.StringUtils;

import java.beans.IntrospectionException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TaosConsumer<V> implements TConsumer<V> {

    private static final long NO_CURRENT_THREAD = -1L;
    // currentThread holds the threadId of the current thread accessing
    // used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private volatile boolean closed = false;

    private Deserializer<V> deserializer;

    long resultSet;
    private final TMQConnector connector;
    private OffsetCommitCallback callback;
    List<V> list = new ArrayList<>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(10),
            r -> {
                Thread t = new Thread(r);
                t.setName("consumer-callback-" + t.getId());
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * Note: after creating a {@link TaosConsumer} you must always {@link #close()}
     * it to avoid resource leaks.
     */
    @SuppressWarnings("unchecked")
    public TaosConsumer(Properties properties) throws SQLException, IntrospectionException, ClassNotFoundException {
        connector = new TMQConnector();
        if (null == properties)
            throw TSDBError.createSQLException(TMQConstants.TMQ_CONF_NULL, "consumer properties must not be null!");

        String servers = properties.getProperty(TMQConstants.BOOTSTRAP_SERVERS);
        if (!StringUtils.isEmpty(servers)) {
            // TODO HOW TO CONNECT WITH TDengine CLUSTER
            Arrays.stream(servers.split(",")).filter(s -> !StringUtils.isEmpty(s))
                    .findFirst().ifPresent(s -> {
                        String[] host = s.split(":");
                        properties.setProperty(TMQConstants.CONNECT_IP, host[0]);
                        properties.setProperty(TMQConstants.CONNECT_PORT, host[1]);
                    });
        }

        String s = properties.getProperty(TMQConstants.VALUE_CLASS);
        if (!StringUtils.isEmpty(s)) {
            deserializer = (Deserializer<V>) new ReferenceDeserializer<>(Class.forName(s));
        } else {
            deserializer = (Deserializer<V>) new MapDeserializer();
        }

        deserializer.configure(properties);
        long config = connector.createConfig(properties);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    public void commitCallbackHandler(int code) {
        CallbackResult r = new CallbackResult(code, list);
        if (TMQConstants.TMQ_SUCCESS != code) {
            Exception exception = TSDBError.createSQLException(code, connector.getErrMsg(code));
            executor.submit(() -> callback.onComplete(r, exception));
        } else {
            executor.submit(() -> callback.onComplete(r, null));
        }

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
    public ConsumerRecords<V> poll(Duration timeout) throws SQLException {
        acquireAndEnsureOpen();
        try {
            resultSet = connector.poll(timeout.toMillis());
            list = new ArrayList<>();
            // when tmq pointer is null or result set is null
            if (resultSet == 0 || resultSet == TMQConstants.TMQ_CONSUMER_NULL) {
                return ConsumerRecords.empty();
            }

            int timestampPrecision = connector.getResultTimePrecision(resultSet);

            Map<TopicPartition, List<V>> records = new HashMap<>();
            TopicPartition partition = null;
            try (TMQResultSet rs = new TMQResultSet(connector, resultSet, timestampPrecision)) {
                while (rs.next()) {
                    String topic = connector.getTopicName(resultSet);
                    String dbName = connector.getDbName(resultSet);
                    int vgroupId = connector.getVgroupId(resultSet);
                    String tableName = connector.getTableName(resultSet);
                    TopicPartition tmp = new TopicPartition(topic, dbName, vgroupId, tableName);

                    if (!tmp.equals(partition)) {
                        records.put(partition, list);
                        partition = tmp;
                        list = new ArrayList<>();
                    }
                    try {
                        V record = deserializer.deserialize(rs);
                        list.add(record);
                    } catch (Exception e) {
                        throw new DeserializerException("Deserializer error", e);
                    }
                }
            }
            records.put(partition, list);
            return new ConsumerRecords<>(records);
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
    public void commitAsync(OffsetCommitCallback callback) {
        // currently offset is zero
        this.callback = callback;
        connector.asyncCommit(0, this);
    }


    @Override
    public void commitSync() throws SQLException {
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
            executor.shutdown();
            connector.closeConsumer();
        } finally {
            closed = true;
            release();
        }
    }
}
