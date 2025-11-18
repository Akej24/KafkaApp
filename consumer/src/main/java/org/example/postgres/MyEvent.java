package org.example.postgres;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public record MyEvent(
        String key,
        String value,
        long timestamp,
        String topic,
        int partition,
        long offset
) {
    public MyEvent(ConsumerRecord<String, String> r) {
        this(r.key(), r.value(), r.timestamp(), r.topic(), r.partition(), r.offset());
    }

    public interface SqlColumns {
        String KEY = "key";
        String VALUE = "value";
        String TIMESTAMP = "timestamp";
        String TOPIC = "topic";
        String PARTITION = "partition";
        String OFFSET = "offset";
    }
}