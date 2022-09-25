package ar.cpfw.jqueue.push;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;
import com.fasterxml.uuid.Generators;
import ar.cpfw.jqueue.JQueueException;

class JdbcJQueue implements JTxQueue {

  private static final String DATA_SOURCE_IS_NECESARY =
      "An instance of javax.sql.DataSource is necesary";
  private Connection conn;
  private String channel;
  private static final String DEFAULT_CHANNEL = "default";
  private static final String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";
  private final String tableName;

  public JdbcJQueue(final DataSource dataSource, String tableName) {
    Objects.requireNonNull(dataSource, DATA_SOURCE_IS_NECESARY);
    this.tableName = tableName;
    try {
      this.conn = dataSource.getConnection();
    } catch (SQLException e) {
      throw new JQueueException(e,
          "java.sql.Connection could not be obtained from the dataSource");
    }
  }

  public JdbcJQueue(final Connection conn, String tableName) {
    Objects.requireNonNull(conn, DATA_SOURCE_IS_NECESARY);

    this.conn = conn;
    this.tableName = tableName;
  }

  @Override
  public void push(final String data) {
    Objects.requireNonNull(data, "data must not be null");

    final var channel = this.channel != null ? this.channel : DEFAULT_CHANNEL;
    final var table =
        this.tableName != null ? this.tableName : QUEUE_TABLE_NAME;

    try {
      PreparedStatement st = this.conn.prepareStatement("insert into " + table
          + " (id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)");

      UUID uuid = Generators.timeBasedGenerator().generate();

      st.setString(1, uuid.toString());
      st.setString(2, channel);
      st.setString(3, data);
      st.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
      st.executeUpdate();

    } catch (Exception e) {
      throw new JQueueException(e, "push cannot be done");
    }
  }

  @Override
  public JTxQueue channel(String channelName) {
    this.channel = channelName;
    return this;
  }

}
