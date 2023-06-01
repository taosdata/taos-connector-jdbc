package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_TMQ_CONSUMER_NULL;
import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_TMQ_VGROUP_NOT_FOUND;

public class JNIConsumer<V> implements Consumer<V> {

    private final TMQConnector connector;
    // use in auto commit is false, include history offset
    private final List<ConsumerRecords<V>> offsetList = new ArrayList<>();
    private boolean autoCommit;
    private final Map<Long, OffsetWaitCallback<V>> callbacks = new HashMap<>();

    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(10),
            r -> {
                Thread t = new Thread(r);
                t.setName("consumer-callback-" + t.getId());
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

    public JNIConsumer() {
        connector = new TMQConnector();
    }

    @Override
    public void create(Properties properties) throws SQLException {
        this.autoCommit = Boolean.parseBoolean(properties.getProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false"));
        long config = connector.createConfig(properties);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    @Override
    public void subscribe(Collection<String> topics) throws SQLException {
        long topicPointer = 0L;
        try {
            topicPointer = connector.createTopic(topics);
            connector.subscribe(topicPointer);
        } finally {
            if (topicPointer != TSDBConstants.JNI_NULL_POINTER) {
                connector.destroyTopic(topicPointer);
            }
        }
    }

    @Override
    public void unsubscribe() throws SQLException {
        for (ConsumerRecords<V> cr : offsetList) {
            this.releaseResultSet(cr.getOffset());
        }
        for (Long offset : callbacks.keySet()) {
            this.releaseResultSet(offset);
        }
        connector.unsubscribe();
    }

    @Override
    public Set<String> subscription() throws SQLException {
        return connector.subscription();
    }

    @Override
    public ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException {
        long resultSet = connector.poll(timeout.toMillis());
        // when tmq pointer is null or result set is null
        if (resultSet == 0 || resultSet == ERROR_TMQ_CONSUMER_NULL) {
            return ConsumerRecords.emptyRecord();
        }

        int timestampPrecision = connector.getResultTimePrecision(resultSet);

        ConsumerRecords<V> records = new ConsumerRecords<>(resultSet);
        try (TMQResultSet rs = new TMQResultSet(connector, resultSet, timestampPrecision)) {
            while (rs.next()) {
                String topic = connector.getTopicName(resultSet);
                String dbName = connector.getDbName(resultSet);
                int vGroupId = connector.getVgroupId(resultSet);
                long offset = connector.getOffset(resultSet);
                TopicPartition tp = new TopicPartition(topic, dbName, vGroupId);

                V v = deserializer.deserialize(rs, topic, dbName);
                ConsumerRecord<V> r = new ConsumerRecord<>(topic, dbName, vGroupId, offset, v);
                records.put(tp, r);
            }
        }

        if (autoCommit) {
            this.releaseResultSet(resultSet);
        } else {
            offsetList.add(records);
        }
        return records;
    }

    @Override
    public void commitAsync(OffsetCommitCallback<V> callback) {
        for (ConsumerRecords<V> r : offsetList) {
            OffsetWaitCallback<V> offset = new OffsetWaitCallback<>(r, this, callback);

            connector.asyncCommit(r.getOffset(), offset);
            callbacks.put(r.getOffset(), offset);
        }
        offsetList.clear();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        connector.seek(partition.getTopic(), partition.getVGroupId(), offset);
    }

    @Override
    public long position(TopicPartition partition) {
       return connector.getTopicAssignment(partition.getTopic()).stream()
                .filter(a -> a.getVgId() == partition.getVGroupId())
                .findFirst()
                .orElseThrow(() -> TSDBError.createIllegalStateException(ERROR_TMQ_VGROUP_NOT_FOUND))
                .getCurrentOffset();
    }

    @Override
    public Map<TopicPartition, Long> position(String topic) {
        return connector.getTopicAssignment(topic).stream()
                .collect(HashMap::new
                        , (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getCurrentOffset())
                        , HashMap::putAll
                );
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(String topic) {
        return connector.getTopicAssignment(topic).stream()
                .collect(HashMap::new
                        , (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getBegin())
                        , HashMap::putAll
                );
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(String topic) {
        return connector.getTopicAssignment(topic).stream()
                .collect(HashMap::new
                        , (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getEnd())
                        , HashMap::putAll
                );
    }

    @Override
    public void commitSync() throws SQLException {
        for (ConsumerRecords<V> r : offsetList) {
            connector.syncCommit(r.getOffset());
            this.releaseResultSet(r.getOffset());
        }
        offsetList.clear();
    }

    @Override
    public void close() throws SQLException {
        executor.shutdown();

        for (ConsumerRecords<V> cr : offsetList) {
            this.releaseResultSet(cr.getOffset());
        }
        for (Long offset : callbacks.keySet()) {
            this.releaseResultSet(offset);
        }
        connector.closeConsumer();
    }

    public void releaseResultSet(long ptr) throws SQLException {
        int code = this.connector.freeResultSet(ptr);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        }
    }

    public String getErrMsg(int code) {
        return connector.getErrMsg(code);
    }

    public synchronized void closeOffset(long prt) {
        callbacks.remove(prt);
    }
}
