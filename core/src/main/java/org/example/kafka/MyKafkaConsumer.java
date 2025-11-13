package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.postgres.MyEvent;
import org.example.postgres.MyEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaConsumer.class);
    private static final String GROUP_ID_PREFIX = "session-";
    private final MyEventRepository myEventRepository;
    private final String bootstrapServers;
    private final String topic;
    private final int partition;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MyKafkaConsumer(final MyEventRepository myEventRepository, final String bootstrapServers, final String topic, final int partition) {
        this.myEventRepository = myEventRepository;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.partition = partition;
    }

    public void cliAndInfinityConsuming(final Scanner scanner) {
        LOG.info("Current table 'events' entries: {}", myEventRepository.selectAll().size());
        System.out.println("Enter consumer ID:");
        final var consumerId = scanner.nextLine();
        System.out.println("Enter timestamp (optional):");
        final var timestamp = scanner.nextLine();
        new Thread(() -> consume(consumerId, parseTimestamp(timestamp))).start();
    }

    private void consume(final String consumerId, final long timestamp) {
        try (final var consumer = new KafkaConsumer<String, String>(getProperties(consumerId))) {
            addShutdownHook(consumer);
            subscribeToTopicAndMoveToOffsetIfTimestampFound(timestamp, consumer);
            while (running.get()) processEvents(consumer.poll(Duration.ofMillis(500)), consumer);
        } catch (final Exception e) {
            LOG.error("Failed to consume message", e);
        }
    }

    private void subscribeToTopicAndMoveToOffsetIfTimestampFound(final long timestamp, final KafkaConsumer<String, String> consumer) {
        if (timestamp <= 0) {
            consumer.subscribe(Collections.singleton(topic));
            return;
        }
        consumer.assign(Collections.singletonList(new TopicPartition(topic, partition)));
        final var tp = new TopicPartition(topic, partition);
        Optional.ofNullable(consumer.offsetsForTimes(Map.of(tp, timestamp)).get(tp))
                .ifPresent(result -> consumer.seek(tp, result.offset()));
    }

    private void processEvents(final ConsumerRecords<String, String> records, final KafkaConsumer<String, String> consumer) {
        records.forEach(r -> {
            if (myEventRepository.insert(new MyEvent(r))) {
                LOG.info("Event '{}' persisted", r.key());
                consumer.commitSync();
            }
        });
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
        if (timestamp == null || timestamp.isEmpty()) return 0;
        try {
            return Long.parseLong(timestamp);
        } catch (final NumberFormatException e) {
            return 0;
        }
    }

    private void addShutdownHook(final KafkaConsumer<String, String> consumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received — waking up consumer...");
            running.set(false);
            consumer.wakeup();
        }));
    }
}
