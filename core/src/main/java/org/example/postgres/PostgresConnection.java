package org.example.postgres;

import org.example.config.AppConfig;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class PostgresConnection {

    public static DataSource getDataSource(final AppConfig config) {
        final var dataSource = new PGSimpleDataSource();
        dataSource.setURL(connectionString(
                config.get(AppConfig.Props.POSTGRES_USERNAME.key()).orElse(""),
                config.get(AppConfig.Props.POSTGRES_PASSWORD.key()).orElse(""),
                config.get(AppConfig.Props.POSTGRES_HOST.key()).orElse(""),
                config.get(AppConfig.Props.POSTGRES_PORT.key()).orElse(""),
                config.get(AppConfig.Props.POSTGRES_DATABASE.key()).orElse("")
        ));
        dataSource.setDatabaseName(config.get(AppConfig.Props.POSTGRES_DATABASE.key()).orElse(""));
        dataSource.setUser(config.get(AppConfig.Props.POSTGRES_USERNAME.key()).orElse(""));
        dataSource.setPassword(config.get(AppConfig.Props.POSTGRES_PASSWORD.key()).orElse(""));
        return dataSource;
    }

    private static String connectionString(
            final String username,
            final String password,
            final String host,
            final String port,
            final String databaseName
    ) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + databaseName + "?user=" + username + "&password=" + password;
    }
}
