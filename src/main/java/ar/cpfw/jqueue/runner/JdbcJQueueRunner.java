package ar.cpfw.jqueue.runner;

import ar.cpfw.jqueue.JQueueException;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Objects;

class JdbcJQueueRunner implements JQueueRunner {

    private static final int JOBID_COLUMN = 3;
    private static final int FIVE_MINUTES = 5;
    private static final int ATTEMPT_COLUMN = 3;
    private static final int DATA_COLUMN = 2;
    private static final int PK_COLUMN = 1;
    private static final String DEFAULT_CHANNEL = "default";
    private static final String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";
    private DataSource dataSource;
    private final String tableName;
    private String channel;
    private ConnStr connStr;

    public JdbcJQueueRunner(String url, String user, String pwd, final String table) {
        assertConnStrIsNotNull(url, user, pwd);
        assertConnStrIsNotBlank(url, user, pwd);
        this.connStr = new ConnStr(url, user, pwd);
        this.tableName = table;
    }

    public JdbcJQueueRunner(final DataSource source, final String table) {
        Objects.requireNonNull(source, "dataSource must not be null");
        this.dataSource = source;
        this.tableName = table;
    }

    @Override
    public void executeAll(final Job job) {
        try {
            doExectute(job);
        } catch (Exception e) {
            throw new JQueueException(e, "executeAll could not be done");
        }
    }

    private void doExectute(final Job job) throws Exception {
        final var channelName =
                this.channel != null ? this.channel : DEFAULT_CHANNEL;
        final var table =
                this.tableName != null ? this.tableName : QUEUE_TABLE_NAME;
        final var conn = this.dataSource != null
                ? this.dataSource.getConnection() : getConn();
        final var queryBuilder = QueryBuilder.build(conn, table);

        try {
            conn.setAutoCommit(false);

            while (true) {
                final ResultSet resultSet =
                        readNextJob(channelName, conn, queryBuilder);
                if (!resultSet.next()) {
                    break;
                }
                final Long jobId = resultSet.getLong(PK_COLUMN);
                final String jobData = resultSet.getString(DATA_COLUMN);
                final int currentAttempt = resultSet.getInt(ATTEMPT_COLUMN);
                try {
                    job.run(jobData);
                    deleteExecutedJob(conn, jobId, queryBuilder);
                } catch (Exception exception) {
                    pushBackFailedJob(conn, jobId, currentAttempt, queryBuilder);
                }
                resultSet.close();
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

    private Connection getConn() {
        try {
            return DriverManager.getConnection(this.connStr.url()
                    , this.connStr.user(), this.connStr.pwd());
        } catch (SQLException e) {
            throw new JQueueException(e, "java.sql.Connection could not be obtained");
        }
    }

    private void pushBackFailedJob(final Connection conn, final Long jobId,
                                   final int currentAttempt, final QueryBuilder queryBuilder)
            throws SQLException {
        final PreparedStatement st =
                conn.prepareStatement(queryBuilder.updateQueryOnFail());
        st.setInt(1, currentAttempt + 1);
        st.setInt(2, FIVE_MINUTES * (currentAttempt + 1));
        st.setLong(JOBID_COLUMN, jobId);
        st.executeUpdate();
        st.close();
    }

    private void deleteExecutedJob(final Connection conn, final Long jobId,
                                   final QueryBuilder queryBuilder) throws SQLException {
        final PreparedStatement st =
                conn.prepareStatement(queryBuilder.deleteQueryOnSuccess());
        st.setLong(1, jobId);
        st.executeUpdate();
        st.close();
    }

    private ResultSet readNextJob(final String channelName, final Connection conn,
                                  final QueryBuilder queryBuilder) throws SQLException {
        final PreparedStatement st =
                conn.prepareStatement(queryBuilder.readQuery());

        final var time = Timestamp.valueOf(LocalDateTime.now());
        st.setString(1, channelName);
        st.setTimestamp(2, time);

        return st.executeQuery();
    }

    @Override
    public JQueueRunner channel(final String channelName) {
        this.channel = channelName;
        return this;
    }

    private void assertConnStrIsNotNull(String url, String user, String pwd) {
        Objects.requireNonNull(url, "url must not be null");
        Objects.requireNonNull(user, "user must not be null");
        Objects.requireNonNull(pwd, "pwd must not be null");
    }

    private void assertConnStrIsNotBlank(String url, String user, String pwd) {
        if (url.isBlank() || user.isBlank() || pwd.isBlank()) {
            throw new IllegalArgumentException("url, user and pwd must not be blank");
        }
    }
}

record ConnStr(String url, String user, String pwd) {
}