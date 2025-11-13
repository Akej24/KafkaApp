package org.example;

import org.example.config.AppConfig;
import org.example.kafka.KafkaManager;
import org.example.kafka.MyKafkaConsumer;
import org.example.kafka.MyKafkaProducer;
import org.example.postgres.MyEventRepository;
import org.example.postgres.PostgresConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.Executors;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String PROPERTIES_FILE_NAME = "app.properties";

    public static void main(String[] args) {
        final var config = AppConfig.register(PROPERTIES_FILE_NAME);
        final var kafkaFuture = KafkaManager.prepareInstance(config, "Thread-Kafka-" + Thread.currentThread().threadId());

        LOG.info("Creating Kafka instance...");
        try (final var kafka = kafkaFuture.join(); final var scanner = new Scanner(System.in)) {

            LOG.info("Kafka broker running at: {}", kafka.getBootstrapServers());
            final var eventRepo = new MyEventRepository(PostgresConnection.getDataSource(config), config);
            final var producer = new MyKafkaProducer(kafka.getBootstrapServers(), kafka.getTopicEvents(config));
            final var consumer = new MyKafkaConsumer(eventRepo, kafka.getBootstrapServers(), kafka.getTopicEvents(config), kafka.getPartitionNumber());

            LOG.info("Creating 'events' table...");
            eventRepo.createEventsTable();

            LOG.info("Producing test messages...");
            Executors.newSingleThreadExecutor().execute(producer::produceTestScheduledMessages);
            while (true) consumer.cliAndInfinityConsuming(scanner);

        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
