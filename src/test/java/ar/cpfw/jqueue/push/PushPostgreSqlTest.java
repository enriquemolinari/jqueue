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
  public void setUp() throws SQLException {
    var ds = this.pgDataSource();

    new JdbcSession(ds).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue").execute();

    new JdbcSession(ds).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  public void pushIsRolledBackIfTxFails() throws SQLException {
    new PushUseCases(pgDataSource()).pushIsRolledBackIfTxFails();
  }

  @Test
  public void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new PushUseCases(pgDataSource()).pushCanBeDoneBySpecifyingAChannel();
  }

  @Test
  public void pushCanBeDoneInDefaultChannel() throws SQLException {
    new PushUseCases(pgDataSource()).pushCanBeDoneInDefaultChannel();
  }

  private DataSource pgDataSource() {
    final PGSimpleDataSource src = new PGSimpleDataSource();
    src.setUrl(this.container.getJdbcUrl());
    src.setUser(this.container.getUsername());
    src.setPassword(this.container.getPassword());
    return src;
  }
}
