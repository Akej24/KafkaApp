package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MyKafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaProducer.class);
    private static final String ACK_ALL_REPLICAS_IN_SYNC = "all";
    private final KafkaProducer<String, String> producer;
    private final String bootstrapServers;
    private final String topic;

    public MyKafkaProducer(final String bootstrapServers, final String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.producer = new KafkaProducer<>(getProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeProducer));
    }

    public void produceTestScheduledMessages() {
        try (final var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.schedule(() -> produceBatch(5), 5, TimeUnit.SECONDS);
            scheduler.schedule(() -> produceAnotherBatch(5), 9, TimeUnit.SECONDS);
        } catch (final Exception e) {
            LOG.error("Failed to schedule test messages: {}", e.getMessage());
        }
    }

    private void produceBatch(final int count) {
        IntStream.range(0, count)
                .mapToObj(i -> Map.entry(UUID.randomUUID() + "#" + i, "Something happened#" + i))
                .forEach(e -> produceMessage(e.getKey(), e.getValue()));
    }

    private void produceAnotherBatch(final int count) {
        IntStream.range(0, count)
                .mapToObj(i -> Map.entry(UUID.randomUUID() + "#" + i, "Something else happened#" + i))
                .forEach(e -> produceMessage(e.getKey(), e.getValue()));
    }

    private void produceMessage(final String key, final String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value), logRecordCallback(key, value));
        } catch (final Exception e) {
            LOG.error("Failed to produce message: {}", e.getMessage());
        }
    }

    private Callback logRecordCallback(final String key, final String value) {
        return (metadata, e) -> {
            if (e != null)
                LOG.error("Failed to send: {} partition: {} offset: {}", key, metadata.partition(), metadata.offset());
            else
                LOG.info("Produced key={} value={}", key, value);
        };
    }

    private Properties getProperties() {
        final var properties = new Properties();
        properties.putAll(Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.ACKS_CONFIG, ACK_ALL_REPLICAS_IN_SYNC),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString())));
        return properties;
    }

    private void closeProducer() {
        try {
            producer.flush();
            producer.close();
            LOG.info("KafkaProducer closed");
        } catch (final Exception e) {
            LOG.error("Error closing producer: {}", e.getMessage());
        }
    }
}
