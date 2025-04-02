package ar.cpfw.jqueue.runner;

import com.jcabi.jdbc.JdbcSession;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RunnerUseCases {

    private static final String JOB_FAILED =
            "something went wrong with the job...";
    private DataSource dataSource;
    private ConnStr connStr;

    public RunnerUseCases(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public RunnerUseCases(String url, String user, String pass) {
        this.connStr = new ConnStr(url, user, pass);
    }

    public void runnerWorksWithOneJob(DataSource dataSource1) throws SQLException {
        new JdbcSession(dataSource1).sql("insert into ar_cpfw_jqueue "
                        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
                .set("default").set("Hello World")
                .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(1))).execute();

        final var runner = getRunner();
        runner.executeAll(data -> assertEquals("Hello World", data));

        final int totalRows = new JdbcSession(dataSource1)
                .sql("select count(*) from ar_cpfw_jqueue")
                .select((rset, stmt) -> {
                    rset.next();
                    return rset.getInt(1);
                });
        assertEquals(0, totalRows);
    }

    private @NotNull JQueueRunner getRunner() {
        if (this.dataSource == null) {
            return JQueueRunner.runner(this.connStr.url(), this.connStr.user(), this.connStr.pwd());
        }
        return JQueueRunner.runner(this.dataSource);
    }

    public void failJobIsNotExecutedYet(DataSource dataSource1) throws SQLException {
        new JdbcSession(dataSource1).sql("insert into ar_cpfw_jqueue "
                        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
                .set("default").set("wrongJob")
                .set(Timestamp.valueOf(LocalDateTime.now())).execute();

        var runner = getRunner();
        runner.executeAll(data -> {
            throw new RuntimeException(JOB_FAILED);
        });

        var runner2 = getRunner();
        runner2.executeAll(data -> fail("The job should not be executed"));

        final int totalRows = new JdbcSession(dataSource1)
                .sql("select count(*) from ar_cpfw_jqueue")
                .select((rset, stmt) -> {
                    rset.next();
                    return rset.getInt(1);
                });

        assertEquals(1, totalRows);
    }

    public void jobThatFailsIsPushedBack(DataSource dataSource1) throws SQLException {
        new JdbcSession(dataSource1).sql("insert into ar_cpfw_jqueue "
                        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
                .set("default").set("wrongJob")
                .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(2))).execute();

        var runner = getRunner();
        runner.executeAll(data -> {
            throw new RuntimeException(JOB_FAILED);
        });

        Map<String, String> outcome = new JdbcSession(dataSource1)
                .sql("select channel, data, attempt, delay from ar_cpfw_jqueue")
                .select((rset, stmt) -> {

                    rset.next();
                    return Map.of("channel", rset.getString(1), "data",
                            rset.getString(2), "attempt", String.valueOf(rset.getInt(3)),
                            "delay", String.valueOf(rset.getInt(4)));
                });

        assertEquals("default", outcome.get("channel"));
        assertEquals("wrongJob", outcome.get("data"));
        assertEquals("1", outcome.get("attempt"));
        assertEquals("5", outcome.get("delay"));
    }

    public void runnerWorksWithTwoJobs(DataSource dataSource1) throws SQLException {
        new JdbcSession(dataSource1).sql("insert into ar_cpfw_jqueue "
                        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
                .set("default").set("FirstJob")
                .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(3))).execute();

        new JdbcSession(dataSource1).sql("insert into ar_cpfw_jqueue"
                        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
                .set("default").set("SecondJob")
                .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(1))).execute();

        var jobsData = new ArrayList<String>();

        var runner = getRunner();
        runner.executeAll(data -> jobsData.add(data));

        assertEquals(2, jobsData.size());
        assertEquals("FirstJob", jobsData.get(0));
        assertEquals("SecondJob", jobsData.get(1));

        final int totalRows = new JdbcSession(dataSource1)
                .sql("select count(*) from ar_cpfw_jqueue")
                .select((rset, stmt) -> {
                    rset.next();
                    return rset.getInt(1);
                });

        assertEquals(0, totalRows);
    }
}
