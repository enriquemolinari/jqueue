package ar.cpfw.jqueue.runner;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.jcabi.jdbc.JdbcSession;

@Testcontainers(disabledWithoutDocker = true)
public class RunnerPgTest {

  /**
   * The database container.
   */
  @Container
  private final JdbcDatabaseContainer<?> container =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest")
          .asCompatibleSubstituteFor("postgres"));

  @BeforeEach
  void setUp() throws SQLException {
    final var dataSource = this.source();

    new JdbcSession(dataSource).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue")
        .execute();

    new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id serial,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  void runnerWorksWithOneJob() throws SQLException {
    new RunnerUseCases(source()).runnerWorksWithOneJob();
  }

  @Test
  void failJobIsNotExecutedYet() throws SQLException {
    new RunnerUseCases(source()).failJobIsNotExecutedYet();
  }

  @Test
  void jobThatFailsIsPushedBack() throws SQLException {
    new RunnerUseCases(source()).jobThatFailsIsPushedBack();
  }

  @Test
  void runnerWorksWithTwoJobs() throws SQLException {
    new RunnerUseCases(source()).runnerWorksWithTwoJobs();
  }


  private DataSource source() {
    final PGSimpleDataSource src = new PGSimpleDataSource();
    src.setUrl(this.container.getJdbcUrl());
    src.setUser(this.container.getUsername());
    src.setPassword(this.container.getPassword());
    return src;
  }

}
