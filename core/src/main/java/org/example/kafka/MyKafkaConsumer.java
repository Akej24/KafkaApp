package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
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
    private final MyEventRepository myEventRepository;
    private final String bootstrapServers;
    private final String topic;
    private final int partition;

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
        consume(consumerId, parseTimestamp(timestamp));
    }

    private void consume(final String consumerId, final long timestamp) {
        final var tp = new TopicPartition(topic, partition);
        try (final var consumer = new KafkaConsumer<String, String>(getProperties(consumerId))) {
            addShutdownHook(consumer);
            consumer.subscribe(Collections.singleton(topic), rebalanceListener(timestamp, consumer));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            while (!records.isEmpty()) {
                processEvents(records, consumer, tp);
                records = consumer.poll(Duration.ofMillis(500));
            }
        } catch (final Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private ConsumerRebalanceListener rebalanceListener(final long timestamp, final KafkaConsumer<String, String> consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (timestamp <= 0) return;
                final var query = new HashMap<TopicPartition, Long>();
                partitions.forEach(topicPartition -> query.put(topicPartition, timestamp));
                consumer.offsetsForTimes(query).forEach((tp, ot) -> {
                    if (ot != null) consumer.seek(tp, ot.offset());
                });
            }
        };
    }

    private void processEvents(final ConsumerRecords<String, String> records, final KafkaConsumer<String, String> consumer, final TopicPartition tp) throws RuntimeException {
        records.forEach(record -> {
            if (Math.random() < 0.2)
                throw new RuntimeException("[Simulation] Random runtime exception (e.g. database connection is broken)");
            delay(500);
            if (myEventRepository.insert(new MyEvent(record))) {
                LOG.info("Event '{}' persisted", record.key());
                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
            }
        });
    }

    private Properties getProperties(final String consumerId) {
        final var properties = new Properties();
        properties.putAll(Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_PREFIX + consumerId),
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

    private static void delay(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void addShutdownHook(final KafkaConsumer<String, String> consumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received - waking up consumer...");
            consumer.wakeup(); // it throws WakeupException (so we can handle it in a consumer, interrupts poll() method)
        }));
    }
}
