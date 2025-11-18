package org.example;

import org.example.config.AppConfig;
import org.example.kafka.MyKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String PROPERTIES_FILE_NAME = "app.properties";

    public static void main(String[] args) {
        final var config = AppConfig.register(PROPERTIES_FILE_NAME);
        final var producer = new MyKafkaProducer(config);
        LOG.info("Producing test messages...");
        producer.produceTestScheduledMessages();
    }
}
