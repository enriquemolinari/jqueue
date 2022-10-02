package ar.cpfw.jqueue.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import javax.sql.DataSource;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class RunnerUseCases {

  private static final String JOB_FAILED =
      "something went wrong with the job...";
  private DataSource dataSource;

  public RunnerUseCases(final DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void runnerWorksWithOneJob() throws SQLException {
    new JdbcSession(this.dataSource).sql("insert into ar_cpfw_jqueue "
        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
        .set("default").set("Hello World")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(1))).execute();

    final var runner = JQueueRunner.runner(this.dataSource);
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        assertEquals("Hello World", data);
      }
    });

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
        .select(new Outcome<Integer>() {
          @Override
          public Integer handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getInt(1);
          }
        });
    assertEquals(0, totalRows);
  }

  public void failJobIsNotExecutedYet() throws SQLException {
    new JdbcSession(this.dataSource).sql("insert into ar_cpfw_jqueue "
        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
        .set("default").set("wrongJob")
        .set(Timestamp.valueOf(LocalDateTime.now())).execute();

    var runner = JQueueRunner.runner(this.dataSource);
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        throw new RuntimeException(JOB_FAILED);
      }
    });

    JQueueRunner.runner(this.dataSource);
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        fail("The job should not be executed");
      }
    });

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
        .select(new Outcome<Integer>() {
          @Override
          public Integer handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getInt(1);
          }
        });

    assertEquals(1, totalRows);
  }

  public void jobThatFailsIsPushedBack() throws SQLException {
    new JdbcSession(this.dataSource).sql("insert into ar_cpfw_jqueue "
        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
        .set("default").set("wrongJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(2))).execute();

    var runner = JQueueRunner.runner(this.dataSource);
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        throw new RuntimeException(JOB_FAILED);
      }
    });

    Map<String, String> outcome = new JdbcSession(this.dataSource)
        .sql("select channel, data, attempt, delay from ar_cpfw_jqueue")
        .select(new Outcome<Map<String, String>>() {
          @Override
          public Map<String, String> handle(final ResultSet rset,
              final Statement stmt) throws SQLException {

            rset.next();
            return Map.of("channel", rset.getString(1), "data",
                rset.getString(2), "attempt", String.valueOf(rset.getInt(3)),
                "delay", String.valueOf(rset.getInt(4)));
          }
        });

    assertEquals("default", outcome.get("channel"));
    assertEquals("wrongJob", outcome.get("data"));
    assertEquals("1", outcome.get("attempt"));
    assertEquals("5", outcome.get("delay"));
  }

  public void runnerWorksWithTwoJobs() throws SQLException {
    new JdbcSession(this.dataSource).sql("insert into ar_cpfw_jqueue "
        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
        .set("default").set("FirstJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(3))).execute();

    new JdbcSession(this.dataSource).sql("insert into ar_cpfw_jqueue"
        + "(channel, data, attempt, delay, pushed_at) values (?, ?, null, 0, ?)")
        .set("default").set("SecondJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(1))).execute();

    var jobsData = new ArrayList<String>();

    var runner = JQueueRunner.runner(this.dataSource);
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        jobsData.add(data);
      }
    });

    assertEquals(2, jobsData.size());
    assertEquals("FirstJob", jobsData.get(0));
    assertEquals("SecondJob", jobsData.get(1));

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
        .select(new Outcome<Integer>() {
          @Override
          public Integer handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getInt(1);
          }
        });

    assertEquals(0, totalRows);
  }
}
