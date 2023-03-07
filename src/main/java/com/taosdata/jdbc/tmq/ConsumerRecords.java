package com.taosdata.jdbc.tmq;

import java.util.*;

public class ConsumerRecords<V> implements Iterable<ConsumerRecord<V>> {

    public static final ConsumerRecords<?> EMPTY = new ConsumerRecords<>(Collections.emptyMap());

    private long offset;
    private final Map<TopicPartition, List<ConsumerRecord<V>>> records;

    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<V>>> records) {
        this.records = records;
    }

    public ConsumerRecords(long offset) {
        this.records = new HashMap<>();
        this.offset = offset;
    }

    protected long getOffset() {
        return offset;
    }

    public void put(TopicPartition tp, ConsumerRecord<V> r) {
        if (records.containsKey(tp)) {
            records.get(tp).add(r);
        } else {
            ArrayList<ConsumerRecord<V>> list = new ArrayList<>();
            list.add(r);
            records.put(tp, list);
        }
    }

    public List<ConsumerRecord<V>> get(TopicPartition partition) {
        List<ConsumerRecord<V>> recs = this.records.get(partition);
        if (recs == null) return Collections.emptyList();
        else return Collections.unmodifiableList(recs);
    }

    @Override
    public Iterator<ConsumerRecord<V>> iterator() {
        return new Iterator<ConsumerRecord<V>>() {
            final Iterator<? extends Iterable<ConsumerRecord<V>>> iters = records.values().iterator();
            Iterator<ConsumerRecord<V>> current;

            @Override
            public boolean hasNext() {
                while (current == null || !current.hasNext()) {
                    if (iters.hasNext()) current = iters.next().iterator();
                    else return false;
                }
                return true;
            }

            @Override
            public ConsumerRecord<V> next() {
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
