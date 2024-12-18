package ar.cpfw.jqueue.push;

import ar.cpfw.jqueue.JQueueException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

final class JdbcJQueue implements JTxQueue {

    private static final int PUSHEDAT_COLUMN = 3;
    private static final int DATA_COLUMN = 2;
    private static final int CHANNEL_COLUMN = 1;
    private static final String DS_IS_NECESARY =
            "An instance of javax.sql.DataSource is necesary";
    private static final String DEFAULT_CHANNEL = "default";
    private static final String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";
    private final Connection connection;
    private final String databaseTableName;
    private String channel;

    public JdbcJQueue(final DataSource dataSource, final String tableName) {
        Objects.requireNonNull(dataSource, DS_IS_NECESARY);
        this.databaseTableName = tableName;
        try {
            this.connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new JQueueException(e,
                    "java.sql.Connection could not be obtained from the dataSource");
        }
    }

    public JdbcJQueue(final Connection conn, final String tableName) {
        Objects.requireNonNull(conn, DS_IS_NECESARY);

        this.connection = conn;
        this.databaseTableName = tableName;
    }

    @Override
    public void push(final String data) {
        Objects.requireNonNull(data, "data must not be null");

        final var channelName =
                this.channel != null ? this.channel : DEFAULT_CHANNEL;
        final var table = this.databaseTableName != null ? this.databaseTableName
                : QUEUE_TABLE_NAME;

        try {
            final PreparedStatement st =
                    this.connection.prepareStatement("insert into " + table
                            + " (channel, data, attempt, delay, pushed_at) "
                            + "values (?, ?, null, 0, ?)");

            st.setString(CHANNEL_COLUMN, channelName);
            st.setString(DATA_COLUMN, data);
            st.setTimestamp(PUSHEDAT_COLUMN, Timestamp.valueOf(LocalDateTime.now()));
            st.executeUpdate();

        } catch (SQLException e) {
            throw new JQueueException(e, "push cannot be done");
        }
    }

    @Override
    public JTxQueue channel(final String channelName) {
        this.channel = channelName;
        return this;
    }

}
