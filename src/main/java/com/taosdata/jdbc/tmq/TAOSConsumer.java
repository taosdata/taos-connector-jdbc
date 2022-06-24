package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TAOSConsumer<V> implements TConsumer<V> {

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
     * Note: after creating a {@link TAOSConsumer} you must always {@link #close()}
     * it to avoid resource leaks.
     */
    @SuppressWarnings("unchecked")
    public TAOSConsumer(Properties properties) throws SQLException {
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

        String s = properties.getProperty(TMQConstants.VALUE_DESERIALIZER);
        if (!StringUtils.isEmpty(s)) {
            deserializer = (Deserializer<V>) Utils.newInstance(Utils.parseClassType(s));
        }

        long config = connector.createConfig(properties);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    public void commitCallbackHandler(int code, long offset) {
//        CallbackResult r = new CallbackResult(code, this, offset);
//        Exception exception = null;
//        executor.submit(() -> callback.onComplete(offset, exception));
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
            // when tmq pointer is null or result set is null
            if (resultSet == 0 || resultSet == TMQConstants.TMQ_CONSUMER_NULL) {
                return ConsumerRecords.empty();
            }

            String topic = connector.getTopicName(resultSet);
            String dbName = connector.getDbName(resultSet);
            int vgroupId = connector.getVgroupId(resultSet);
            TopicPartition partition = new TopicPartition(topic, dbName, vgroupId);

            int timestampPrecision = connector.getResultTimePrecision(resultSet);
            TMQResultSet rs = new TMQResultSet(connector, resultSet, timestampPrecision);

            List<ConsumerRecord<V>> list = new ArrayList<>();
            while (rs.next()) {
                try {
                    ConsumerRecord<V> record = new ConsumerRecord<>();
                    record.setKey(connector.getTableName(resultSet));
                    record.setValue(deserializer.deserialize(rs));
                    list.add(record);
                } catch (InstantiationException | IllegalAccessException e) {
                    // ignore
                }
            }

            Map<TopicPartition, List<ConsumerRecord<V>>> records = new HashMap<>();
            records.put(partition, list);
            return new ConsumerRecords<V>(records);
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
