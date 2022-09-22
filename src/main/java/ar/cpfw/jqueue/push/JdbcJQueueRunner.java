package ar.cpfw.jqueue.push;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

class JdbcJQueueRunner implements JQueueRunner {

  private String user;
  private String pwd;
  private String connStr;

  private String channel;
  private String DEFAULT_CHANNEL = "default";

  public JdbcJQueueRunner(String connStr, String user, String pwd) {
    this.user = user;
    this.pwd = pwd;
    this.connStr = connStr;
  }

  @Override
  public void executeAll(Job job) {
    try {
      doExectute(job);
    } catch (Exception e) {
      throw new JQueueException(e, "");
    }
  }

  private void doExectute(Job job) throws Exception {
    var channel = this.channel != null ? this.channel : DEFAULT_CHANNEL;
    var conn = connection();
    var queryBuilder = QueryBuilder.build(conn);

    String jobId = null;
    int currentAttempt = 0;

    try {
      conn.setAutoCommit(false);

      while (true) {
        ResultSet resultSet = readNextJob(channel, conn, queryBuilder);
        if (!resultSet.next()) {
          break;
        }
        jobId = resultSet.getString(1);
        String jobData = resultSet.getString(2);
        currentAttempt = resultSet.getInt(3);
        try {
          job.run(jobData);
          deleteExecutedJob(conn, jobId, queryBuilder);
        } catch (Exception w) {
          if (jobId != null) {
            pushBackToQueueFailedJob(conn, jobId, currentAttempt, queryBuilder);
          }
        }
        conn.commit();
      }
    } catch (SQLException e) {
      conn.rollback();
    } finally {
      conn.setAutoCommit(true);
      conn.close();
    }
  }

  private void pushBackToQueueFailedJob(Connection conn, String jobId, int currentAttempt,
      QueryBuilder queryBuilder) throws SQLException {
    PreparedStatement st = conn.prepareStatement(queryBuilder.updateQueryOnFail());
    st.setInt(1, currentAttempt + 1);
    st.setInt(2, 5 * (currentAttempt + 1)); // minutes
    st.setString(3, jobId);
    st.executeUpdate();
  }

  private void deleteExecutedJob(Connection conn, String jobId, QueryBuilder queryBuilder)
      throws SQLException {
    PreparedStatement st = conn.prepareStatement(queryBuilder.deleteQueryOnSuccess());
    st.setString(1, jobId);
    st.executeUpdate();
  }

  private ResultSet readNextJob(String channel, Connection conn, QueryBuilder queryBuilder)
      throws SQLException {
    PreparedStatement st = conn.prepareStatement(queryBuilder.readQuery());

    var time = Timestamp.valueOf(LocalDateTime.now());
    st.setString(1, channel);
    st.setTimestamp(2, time);

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
