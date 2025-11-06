package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.postgres.MyEvent;
import org.example.postgres.MyEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class MyKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaConsumer.class);
    private static final String GROUP_ID_PREFIX = "session-";
    private final Map<String, Long> sessionOffsets = new HashMap<>();
    private final MyEventRepository myEventRepository;
    private final String bootstrapServers;
    private final String topic;

    public MyKafkaConsumer(final MyEventRepository myEventRepository, final String bootstrapServers, final String topic) {
        this.myEventRepository = myEventRepository;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public void cliAndInfinityConsuming(final Scanner scanner) {
        System.out.println("Enter consumer ID:");
        final var consumerId = scanner.nextLine();
        System.out.println("Enter timestamp (optional):");
        final var timestamp = scanner.nextLine();
        consume(consumerId, parseTimestamp(timestamp));
    }

    private void consume(final String consumerId, final long timestamp) {
        try (var consumer = new KafkaConsumer<String, String>(getProperties(consumerId))) {
            consumer.subscribe(Collections.singletonList(topic));
            LOG.info("Consumer started for {}", consumerId);
            while (true) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> {
                    final MyEvent event = new MyEvent(r.key(), r.value(), r.timestamp(), r.topic(), r.partition(), r.offset());
                    myEventRepository.insert(event);
                    LOG.info("Consumed key={} value={} offset={}", r.key(), r.value(), r.offset());
                });
                LOG.info("Current table 'events' entries: {}", myEventRepository.selectAll().size());
            }
        } catch (Exception e) {
            LOG.error("Failed to consume message", e);
        }
    }

    private Properties getProperties(final String consumerId) {
        final var properties = new Properties();
        properties.putAll(Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_PREFIX + consumerId),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()),
                Map.entry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString())));
        return properties;
    }

    private long parseTimestamp(final String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) return System.currentTimeMillis();
        try {
            return Long.parseLong(timestamp);
        } catch (final NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }
}
