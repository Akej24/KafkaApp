package org.example.kafka;

import org.example.config.AppConfig;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.concurrent.CompletableFuture;

public class KafkaManager implements AutoCloseable {

    private final String bootstrapServers;
    private final EmbeddedKafkaBroker kafka;

    public static CompletableFuture<KafkaManager> prepareInstance(final AppConfig config, final String threadName) {
        return CompletableFuture.supplyAsync(() -> {
            Thread.currentThread().setName(threadName);
            return runInstance(config);
        });
    }

    public static KafkaManager runInstance(final AppConfig config) {
        return new KafkaManager(config);
    }

    private KafkaManager(final AppConfig config) {
        kafka = prepareKafkaBroker(config);
        kafka.afterPropertiesSet();
        bootstrapServers = kafka.getBrokersAsString();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public void close() {
        kafka.destroy();
    }

    private EmbeddedKafkaBroker prepareKafkaBroker(final AppConfig config) {
        return new EmbeddedKafkaBroker(
                config.getInteger(AppConfig.Props.KAFKA_BROKERS.key()).orElse(1),
                true,
                config.getInteger(AppConfig.Props.KAFKA_PARTITIONS.key()).orElse(1),
                config.get(AppConfig.Props.KAFKA_TOPIC_EVENTS.key()).orElse("events")).kafkaPorts(34045);
    }
}