package org.example.postgres;

public record MyEvent(
        String key,
        String value,
        long timestamp,
        String topic,
        int partition,
        long offset
) {
}