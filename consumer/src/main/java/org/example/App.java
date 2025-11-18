package org.example;

import org.example.config.AppConfig;
import org.example.kafka.MyKafkaConsumer;
import org.example.postgres.MyEventRepository;
import org.example.postgres.PostgresConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String PROPERTIES_FILE_NAME = "app.properties";

    public static void main(String[] args) {
        final var config = AppConfig.register(PROPERTIES_FILE_NAME);

        try (final var scanner = new Scanner(System.in)) {
            final var eventRepo = new MyEventRepository(PostgresConnection.getDataSource(config), config);
            final var consumer = new MyKafkaConsumer(eventRepo, config);

            LOG.info("Creating 'events' table...");
            eventRepo.createEventsTable();
            while (true) consumer.cliAndInfinityConsuming(scanner);
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
