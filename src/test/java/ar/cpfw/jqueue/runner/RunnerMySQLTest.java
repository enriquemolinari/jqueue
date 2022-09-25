package ar.cpfw.jqueue.runner;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.jcabi.jdbc.JdbcSession;
import com.mysql.cj.jdbc.MysqlDataSource;

/**
 * If you run on Ubuntu, make sure your user can run docker, if not:
 * 
 * <pre>
 *  sudo groupadd docker sudo
 *  sudo gpasswd -a $USER docker 
 *  then restart your PC
 * </pre>
 */
@Testcontainers(disabledWithoutDocker = true)
public class RunnerMySQLTest {

  /**
   * The database container.
   */
  @Container
  private final JdbcDatabaseContainer<?> container = new MySQLContainer<>(
      DockerImageName.parse("mysql:latest").asCompatibleSubstituteFor("mysql"));

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
  public void runnerWorksWithOneJob() throws SQLException {
    new RunnerUseCases(source()).runnerWorksWithOneJob();
  }

  @Test
  public void failJobIsNotExecutedYet() throws SQLException {
    new RunnerUseCases(source()).failJobIsNotExecutedYet();
  }

  @Test
  public void jobThatFailsIsPushedBack() throws SQLException {
    new RunnerUseCases(source()).jobThatFailsIsPushedBack();
  }

  @Test
  public void runnerWorksWithTwoJobs() throws SQLException {
    new RunnerUseCases(source()).runnerWorksWithTwoJobs();
  }

  private DataSource source() {
    final MysqlDataSource src = new MysqlDataSource();
    src.setUrl(this.container.getJdbcUrl());
    src.setUser(this.container.getUsername());
    src.setPassword(this.container.getPassword());
    return src;
  }
}
