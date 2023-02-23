package com.taosdata.jdbc.tmq;

import java.util.*;

public class ConsumerRecords<V> implements Iterable<V> {

    public static final ConsumerRecords<?> EMPTY = new ConsumerRecords<>(Collections.emptyMap());

    private long offset;
    private final Map<TopicPartition, List<V>> records;

    public ConsumerRecords(Map<TopicPartition, List<V>> records) {
        this.records = records;
    }

    public ConsumerRecords(long offset) {
        this.records = new HashMap<>();
        this.offset = offset;
    }

    protected long getOffset() {
        return offset;
    }

    public void put(TopicPartition tp, V v) {
        if (records.containsKey(tp)) {
            records.get(tp).add(v);
        } else {
            ArrayList<V> list = new ArrayList<>();
            list.add(v);
            records.put(tp, list);
        }
    }

    public List<V> get(TopicPartition partition) {
        List<V> recs = this.records.get(partition);
        if (recs == null)
            return Collections.emptyList();
        else
            return Collections.unmodifiableList(recs);
    }

    @Override
    public Iterator<V> iterator() {
        return new Iterator<V>() {
            final Iterator<? extends Iterable<V>> iters = records.values().iterator();
            Iterator<V> current;

            @Override
            public boolean hasNext() {
                while (current == null || !current.hasNext()) {
                    if (iters.hasNext())
                        current = iters.next().iterator();
                    else
                        return false;
                }
                return true;
            }

            @Override
            public V next() {
                return current.next();
            }
        };
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <T> ConsumerRecords<T> emptyRecord() {
        return (ConsumerRecords<T>) EMPTY;
    }
}
