package ar.cpfw.jqueue.push;

import ar.cpfw.jqueue.JQueueException;

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
    private static final String CONN_IS_NECESARY =
            "An instance of java.sql.Connection is necesary";
    private static final String DEFAULT_CHANNEL = "default";
    private static final String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";
    private final Connection currentInTxConnection;
    private final String databaseTableName;
    private String channel;

    public JdbcJQueue(final Connection currentInTxConnection, final String tableName) {
        Objects.requireNonNull(currentInTxConnection, CONN_IS_NECESARY);
        this.currentInTxConnection = currentInTxConnection;
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
                    this.currentInTxConnection.prepareStatement("insert into " + table
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
