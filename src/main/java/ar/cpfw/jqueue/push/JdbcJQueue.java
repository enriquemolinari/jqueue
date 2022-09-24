package ar.cpfw.jqueue.push;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.sql.DataSource;
import com.fasterxml.uuid.Generators;
import ar.cpfw.jqueue.JQueueException;

class JdbcJQueue implements JTxQueue {

  private Connection conn;
  private String channel;
  private String DEFAULT_CHANNEL = "default";
  private String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";

  public JdbcJQueue(final DataSource dataSource) {
    if (conn == null) {
      throw new JQueueException(
          "An instance of java.sql.Connection is necesary");
    }
    try {
      this.conn = dataSource.getConnection();
    } catch (SQLException e) {
      throw new JQueueException(e,
          "java.sql.Connection could not be obtained from the dataSource");
    }
  }

  public JdbcJQueue(final Connection conn) {
    if (conn == null) {
      throw new JQueueException(
          "An instance of java.sql.Connection is necesary");
    }
    this.conn = conn;
  }

  @Override
  public void push(final String data) {
    if (data == null) {
      throw new JQueueException("data must not be null");
    }

    var channel = this.channel != null ? this.channel : DEFAULT_CHANNEL;

    try {
      PreparedStatement st = this.conn.prepareStatement("insert into "
          + QUEUE_TABLE_NAME
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
