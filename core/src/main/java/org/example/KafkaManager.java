package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Map;
import java.util.Properties;

public class KafkaManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaManager.class);
    private static final int BROKERS = 1;
    private static final int PARTITIONS = 1;
    private static final String TOPIC = "my-test-topic";

    public static void runKafka() throws InterruptedException {
        final var bootstrapServers = startBroker().getBrokersAsString();
        LOG.info("Kafka broker running at: {}", bootstrapServers);
        sendTestMessage(bootstrapServers);
        LOG.info("Message sent. Press Ctrl+C to exit or kill the process.");
        Thread.currentThread().join();
    }

    private static EmbeddedKafkaBroker startBroker() {
        final var embeddedKafka = new EmbeddedKafkaBroker(BROKERS, true, PARTITIONS, TOPIC);
        embeddedKafka.afterPropertiesSet();
        return embeddedKafka;
    }

    private static void sendTestMessage(final String bootstrapServers) {
        final var producer = new KafkaProducer<>(getProperties(bootstrapServers));
        producer.send(new ProducerRecord<>(TOPIC, "key", "Hello from embedded Kafka!"));
        producer.close();
    }

    private static Properties getProperties(final String bootstrapServers) {
        final var properties = new Properties();
        properties.putAll(Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
        return properties;
    }
}