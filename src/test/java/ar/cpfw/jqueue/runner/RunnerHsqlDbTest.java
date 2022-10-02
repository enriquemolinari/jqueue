package ar.cpfw.jqueue.runner;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.jcabi.jdbc.JdbcSession;

public class RunnerHsqlDbTest {

  @BeforeEach
  void setUp() throws SQLException {
    final var dataSource = hSqlDataSource();

    new JdbcSession(dataSource).sql("DROP SCHEMA PUBLIC CASCADE").execute();

    new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id int NOT NULL IDENTITY,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  void runnerWorksWithOneJob() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).runnerWorksWithOneJob();
  }

  @Test
  void failJobIsNotExecutedYet() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).failJobIsNotExecutedYet();
  }

  @Test
  void jobThatFailsIsPushedBack() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).jobThatFailsIsPushedBack();
  }

  @Test
  void runnerWorksWithTwoJobs() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).runnerWorksWithTwoJobs();
  }

  private DataSource hSqlDataSource() {
    final var dataSource = new JDBCDataSource();
    dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    dataSource.setUser("SA");
    dataSource.setPassword("");
    return dataSource;
  }
}
