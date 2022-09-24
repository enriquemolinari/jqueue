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
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.fasterxml.uuid.Generators;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class RunnerHsqlDbTest {

  @BeforeEach
  public void setUp() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).sql("DROP SCHEMA PUBLIC CASCADE").execute();

    new JdbcSession(ds).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  public void runnerWorksWithOneJob() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).sql("insert into ar_cpfw_jqueue "
        + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("Hello World")
        .set(Timestamp.valueOf(LocalDateTime.now())).execute();

    // yyyy-mm-dd hh:mm:ss

    var runner = JQueueRunner.runner(hSqlDataSource());
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        assertEquals("Hello World", data);
      }
    });

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
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

  @Test
  public void failJobIsNotExecutedYet() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).sql("insert into ar_cpfw_jqueue "
        + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("wrongJob")
        .set(Timestamp.valueOf(LocalDateTime.now())).execute();

    var runner = JQueueRunner.runner(hSqlDataSource());
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        throw new RuntimeException("something went wrong with the job...");
      }
    });


    JQueueRunner.runner(hSqlDataSource());
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        // this should not be called
        fail();
      }
    });

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
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

  @Test
  public void jobFailAndIsPushBack() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).sql("insert into ar_cpfw_jqueue "
        + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("wrongJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(2))).execute();

    var runner = JQueueRunner.runner(hSqlDataSource());
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        throw new RuntimeException("something went wrong with the job...");
      }
    });

    Map<String, String> outcome = new JdbcSession(ds)
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

  @Test
  public void runnerWorksWithTwoJobs() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).autocommit(false).sql("insert into ar_cpfw_jqueue "
        + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("FirstJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(2))).execute()
        .sql("insert into ar_cpfw_jqueue"
            + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("SecondJob")
        .set(Timestamp.valueOf(LocalDateTime.now())).execute().commit();

    var jobsData = new ArrayList<String>();

    var runner = JQueueRunner.runner(hSqlDataSource());
    runner.executeAll(new Job() {
      @Override
      public void run(String data) {
        jobsData.add(data);
      }
    });

    assertEquals(2, jobsData.size());
    assertEquals("FirstJob", jobsData.get(0));
    assertEquals("SecondJob", jobsData.get(1));

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
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

  private DataSource hSqlDataSource() {
    final var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    ds.setUser("SA");
    ds.setPassword("");
    return ds;
  }
}
