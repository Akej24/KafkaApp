package org.example.postgres;

import org.example.config.AppConfig;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class MyEventRepository {

    private static final Logger LOG = LoggerFactory.getLogger(MyEventRepository.class);
    private final DataSource dataSource;
    private final AppConfig appConfig;

    public MyEventRepository(final DataSource dataSource, final AppConfig appConfig) {
        this.dataSource = dataSource;
        this.appConfig = appConfig;
    }

    public void createEventsTable() {
        final var sql = appConfig.get(AppConfig.Props.SQL_EVENTS_CREATE.key()).orElse("");
        safeExecute(conn -> conn.createStatement().execute(sql));
    }

    public boolean insert(final MyEvent event) {
        final var sql = appConfig.get(AppConfig.Props.SQL_EVENTS_INSERT.key()).orElse("");
        return safeExecute(conn -> {
            var stmt = conn.prepareStatement(sql);
            stmt.setString(1, event.key());
            stmt.setString(2, event.value());
            stmt.setLong(3, event.timestamp());
            stmt.setString(4, event.topic());
            stmt.setLong(5, event.partition());
            stmt.setLong(6, event.offset());
            stmt.executeUpdate();
        });
    }

    public List<MyEvent> selectAll() {
        final var sql = appConfig.get(AppConfig.Props.SQL_EVENTS_SELECT.key()).orElse("");
        final var result = new ArrayList<MyEvent>();
        safeExecute(conn -> {
            final var rs = conn.prepareStatement(sql).executeQuery();
            while (rs.next()) {
                result.add(new MyEvent(
                        rs.getString(MyEvent.SqlColumns.KEY),
                        rs.getString(MyEvent.SqlColumns.VALUE),
                        rs.getLong(MyEvent.SqlColumns.TIMESTAMP),
                        rs.getString(MyEvent.SqlColumns.TOPIC),
                        rs.getInt(MyEvent.SqlColumns.PARTITION),
                        rs.getLong(MyEvent.SqlColumns.OFFSET)
                ));
            }
        });
        return result;
    }

    private boolean safeExecute(final ThrowingConsumer<Connection> command) {
        try (final var connection = dataSource.getConnection()) {
            command.accept(connection);
            return true;
        } catch (final Throwable e) {
            LOG.error("SQL query failed due to: {}", e.getMessage());
            return false;
        }
    }
}
