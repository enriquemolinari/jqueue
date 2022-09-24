package ar.cpfw.jqueue.runner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;
import javax.sql.DataSource;
import ar.cpfw.jqueue.JQueueException;

class JdbcJQueueRunner implements JQueueRunner {

  private DataSource dataSource;
  private String channel;
  private String DEFAULT_CHANNEL = "default";

  public JdbcJQueueRunner(DataSource dataSource) {
    Objects.requireNonNull(dataSource, "dataSource must not be null");
    this.dataSource = dataSource;
  }

  @Override
  public void executeAll(final Job job) {
    try {
      doExectute(job);
    } catch (Exception e) {
      throw new JQueueException(e, "executeAll all could not be done");
    }
  }

  private void doExectute(final Job job) throws Exception {
    var channel = this.channel != null ? this.channel : DEFAULT_CHANNEL;
    var conn = this.dataSource.getConnection();
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
      throw e;
    } finally {
      conn.setAutoCommit(true);
      conn.close();
    }
  }

  private void pushBackToQueueFailedJob(final Connection conn,
      final String jobId, final int currentAttempt,
      final QueryBuilder queryBuilder) throws SQLException {
    PreparedStatement st =
        conn.prepareStatement(queryBuilder.updateQueryOnFail());
    st.setInt(1, currentAttempt + 1);
    st.setInt(2, 5 * (currentAttempt + 1)); // minutes
    st.setString(3, jobId);
    st.executeUpdate();
  }

  private void deleteExecutedJob(final Connection conn, final String jobId,
      final QueryBuilder queryBuilder) throws SQLException {
    PreparedStatement st =
        conn.prepareStatement(queryBuilder.deleteQueryOnSuccess());
    st.setString(1, jobId);
    st.executeUpdate();
  }

  private ResultSet readNextJob(String channel, Connection conn,
      QueryBuilder queryBuilder) throws SQLException {
    PreparedStatement st = conn.prepareStatement(queryBuilder.readQuery());

    var time = Timestamp.valueOf(LocalDateTime.now());
    st.setString(1, channel);
    st.setTimestamp(2, time);

    ResultSet resultSet = st.executeQuery();
    return resultSet;
  }

  @Override
  public JQueueRunner channel(final String channelName) {
    this.channel = channelName;
    return this;
  }

}
