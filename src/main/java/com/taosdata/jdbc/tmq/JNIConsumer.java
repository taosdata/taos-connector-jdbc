package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.common.Consumer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JNIConsumer<V> implements Consumer<V> {

    private final TMQConnector connector;
    long resultSet;
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

    public JNIConsumer() {
        connector = new TMQConnector();
    }

    @Override
    public void create(Properties properties) throws SQLException {
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
        connector.unsubscribe();
    }

    @Override
    public Set<String> subscription() throws SQLException {
        return connector.subscription();
    }

    @Override
    public ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException {
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
//                String tableName = connector.getTableName(resultSet);
                TopicPartition tmp = new TopicPartition(topic, dbName, vgroupId);

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
    }

    @Override
    public void commitAsync(TaosConsumer<?> consumer) {
        connector.asyncCommit(0, consumer);
    }

    @Override
    public void commitSync() throws SQLException {
        connector.syncCommit(0);
    }

    @Override
    public void close() throws SQLException {
        executor.shutdown();
        connector.closeConsumer();
    }

    @Override
    public void commitCallbackHandler(int code, OffsetCommitCallback callback) {
        CallbackResult r = new CallbackResult(code, list);
        if (TMQConstants.TMQ_SUCCESS != code) {
            Exception exception = TSDBError.createSQLException(code, connector.getErrMsg(code));
            executor.submit(() -> callback.onComplete(r, exception));
        } else {
            executor.submit(() -> callback.onComplete(r, null));
        }
    }
}
