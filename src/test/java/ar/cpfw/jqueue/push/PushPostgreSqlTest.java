package ar.cpfw.jqueue.push;

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
public class PushPostgreSqlTest {

  @Container
  private final JdbcDatabaseContainer<?> container =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest")
          .asCompatibleSubstituteFor("postgres"));

  @BeforeEach
  void setUp() throws SQLException {
    final var dataSource = this.pgDataSource();

    new JdbcSession(dataSource).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue").execute();

    new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  void pushIsRolledBackIfTxFails() throws SQLException {
    new PushUseCases(pgDataSource()).pushIsRolledBackIfTxFails();
  }

  @Test
  void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new PushUseCases(pgDataSource()).pushCanBeDoneBySpecifyingAChannel();
  }

  @Test
  void pushCanBeDoneInDefaultChannel() throws SQLException {
    new PushUseCases(pgDataSource()).pushCanBeDoneInDefaultChannel();
  }

  private DataSource pgDataSource() {
    final PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setUrl(this.container.getJdbcUrl());
    dataSource.setUser(this.container.getUsername());
    dataSource.setPassword(this.container.getPassword());
    return dataSource;
  }
}
