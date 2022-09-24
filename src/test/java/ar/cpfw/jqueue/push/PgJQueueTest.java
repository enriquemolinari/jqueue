package ar.cpfw.jqueue.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.fasterxml.uuid.Generators;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;
import ar.cpfw.jqueue.runner.JQueueRunner;
import ar.cpfw.jqueue.runner.Job;

@Testcontainers(disabledWithoutDocker = true)
public class PgJQueueTest {

  /**
   * The database container.
   */
  @Container
  private final JdbcDatabaseContainer<?> container =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest")
          .asCompatibleSubstituteFor("postgres"));

  @BeforeEach
  public void setUp() throws SQLException {
    var ds = this.source();

    new JdbcSession(ds).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue").execute();

    new JdbcSession(ds).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  public void runnerWorksWithTwoJobs() throws SQLException {
    var ds = this.source();

    new JdbcSession(ds).autocommit(false).sql("insert into ar_cpfw_jqueue "
        + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("FirstJob")
        .set(Timestamp.valueOf(LocalDateTime.now().minusMinutes(2))).execute()
        .sql("insert into ar_cpfw_jqueue "
            + "(id, channel, data, attempt, delay, pushed_at) values (?, ?, ?, null, 0, ?)")
        .set(Generators.timeBasedGenerator().generate().toString())
        .set("default").set("SecondJob")
        .set(Timestamp.valueOf(LocalDateTime.now())).execute().commit();

    var jobsData = new ArrayList<String>();

    var runner = JQueueRunner.runner(this.source());
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

  private DataSource source() {
    final PGSimpleDataSource src = new PGSimpleDataSource();
    src.setUrl(this.container.getJdbcUrl());
    src.setUser(this.container.getUsername());
    src.setPassword(this.container.getPassword());
    return src;
  }

}
