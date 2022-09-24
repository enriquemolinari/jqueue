package ar.cpfw.jqueue.runner;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.jcabi.jdbc.JdbcSession;

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
    new RunnerUseCases(hSqlDataSource()).runnerWorksWithOneJob();
  }

  @Test
  public void failJobIsNotExecutedYet() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).failJobIsNotExecutedYet();
  }

  @Test
  public void jobThatFailsIsPushedBack() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).jobThatFailsIsPushedBack();
  }

  @Test
  public void runnerWorksWithTwoJobs() throws SQLException {
    new RunnerUseCases(hSqlDataSource()).runnerWorksWithTwoJobs();
  }

  private DataSource hSqlDataSource() {
    final var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    ds.setUser("SA");
    ds.setPassword("");
    return ds;
  }
}
