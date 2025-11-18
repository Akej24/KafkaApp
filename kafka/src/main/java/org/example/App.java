package org.example;

import org.example.config.AppConfig;
import org.example.kafka.KafkaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String PROPERTIES_FILE_NAME = "app.properties";

    public static void main(String[] args) {
        final var config = AppConfig.register(PROPERTIES_FILE_NAME);
        final var kafkaFuture = KafkaManager.prepareInstance(config, "Thread-Kafka-" + Thread.currentThread().threadId());

        LOG.info("Creating Kafka instance...");
        try (final var kafka = kafkaFuture.join()) {
            LOG.info("Bootstrap servers: {}", kafka.getBootstrapServers());
            new CountDownLatch(1).await();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
