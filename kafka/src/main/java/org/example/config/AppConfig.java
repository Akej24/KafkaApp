package org.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class AppConfig {

    private final Properties properties = new Properties();

    private AppConfig(final String propertiesFileName) {
        try (final InputStream in = AppConfig.class.getClassLoader().getResourceAsStream(propertiesFileName)) {
            if (in == null) throw new RuntimeException(propertiesFileName + " file not found in resources");
            properties.load(in);
            final var missingKeys = getMissingKeys();
            if (!missingKeys.isEmpty()) {
                throw new IllegalStateException("Missing keys in " + propertiesFileName + ": " + missingKeys);
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load " + propertiesFileName, e);
        }
    }

    public static AppConfig register(final String propertiesFileName) {
        return new AppConfig(propertiesFileName);
    }

    public Optional<String> get(final String key) {
        return Optional.ofNullable(properties.getProperty(key));
    }

    public Optional<Integer> getInteger(final String key) {
        return get(key).flatMap(value -> {
            try {
                return Optional.of(Integer.parseInt(value));
            } catch (final NumberFormatException e) {
                return Optional.empty();
            }
        });
    }

    private List<String> getMissingKeys() {
        return Arrays.stream(Props.values())
                .map(Props::key)
                .filter(key -> !properties.containsKey(key))
                .toList();
    }

    public enum Props {
        KAFKA_BROKERS("kafka.brokers"),
        KAFKA_PARTITIONS("kafka.partitions"),
        KAFKA_TOPIC_EVENTS("kafka.topic.events");

        private final String key;

        Props(final String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }
}
