package ar.cpfw.jqueue.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import ar.cpfw.jqueue.push.JQueueException;
import ar.cpfw.jqueue.push.Job;
import ar.cpfw.jqueue.runner.JQueueRunner;

public class JdbcJQueueRunner implements JQueueRunner {

  private String user;
  private String pwd;
  private String connStr;

  private String channel;
  private String DEFAULT_CHANNEL = "default";
  private String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";

  public JdbcJQueueRunner(String connStr, String user, String pwd) {
    this.user = user;
    this.pwd = pwd;
    this.connStr = connStr;
  }

  @Override
  public void executeAll(Job job) {
    var channel = this.channel != null ? this.channel : DEFAULT_CHANNEL;
    var conn = connection();

    try {
      conn.setAutoCommit(false);

      ResultSet resultSet = readNextJob(channel, conn);
      if (resultSet.next()) {
        String jobId = resultSet.getString(1);
        String jobData = resultSet.getString(2);
        job.run(jobData);
        deleteExecutedJob(conn, jobId);
      }

      conn.commit();
    } catch (Exception e) {
      try {
        conn.rollback();
      } catch (SQLException e1) {
        throw new JQueueException(e1, "executeAll could not be done");
      }
      throw new JQueueException(e, "executeAll could not be done");
    } finally {
      try {
        conn.setAutoCommit(true);
      } catch (SQLException s) {
        throw new JQueueException(s, "executeAll could not be done");
      }
    }
  }

  private void deleteExecutedJob(Connection conn, String jobId) throws SQLException {
    PreparedStatement st = conn.prepareStatement("delete from " + QUEUE_TABLE_NAME + " where id = ?");
    st.setString(1, jobId);
    st.executeUpdate();
  }

  private ResultSet readNextJob(String channel, Connection conn) throws SQLException {
    // TODO: incorporar delay
    // TODO: limit 1 must work in several database vendors
    // TODO: ver timeouts del for update en cada vendor...
    PreparedStatement st = conn.prepareStatement("select id, data from " + QUEUE_TABLE_NAME
        + " where channel = ? and pushed_at <= ? order by pushed_at asc limit 1 for update skip locked");

    st.setString(1, channel);
    st.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));

    ResultSet resultSet = st.executeQuery();
    return resultSet;
  }

  private Connection connection() {
    String url = this.connStr;
    String user = this.user;
    String password = this.pwd;
    try {
      return DriverManager.getConnection(url, user, password);
    } catch (SQLException e) {
      throw new JQueueException(e, "Connection could not be stablished");
    }
  }

  @Override
  public JQueueRunner channel(String channelName) {
    this.channel = channelName;
    return this;
  }

}
