package com.taosdata.jdbc.tmq;

import java.util.*;

public class ConsumerRecords<V> implements Iterable<V> {

    public static final ConsumerRecords<?> EMPTY = new ConsumerRecords(Collections.EMPTY_MAP);

    private final Map<TopicPartition, List<V>> records;

    public ConsumerRecords(Map<TopicPartition, List<V>> records) {
        this.records = records;
    }

    public List<V> records(TopicPartition partition) {
        List<V> recs = this.records.get(partition);
        if (recs == null)
            return Collections.emptyList();
        else
            return Collections.unmodifiableList(recs);
    }

    @Override
    public Iterator iterator() {
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

    @SuppressWarnings("rawtypes")
    public static ConsumerRecords empty() {
        return EMPTY;
    }
}
