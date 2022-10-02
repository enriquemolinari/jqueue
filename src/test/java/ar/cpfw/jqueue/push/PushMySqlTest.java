package ar.cpfw.jqueue.push;

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

@Testcontainers(disabledWithoutDocker = true)
public class PushMySqlTest {

  @Container
  private final JdbcDatabaseContainer<?> container = new MySQLContainer<>(
      DockerImageName.parse("mysql:latest").asCompatibleSubstituteFor("mysql"));

  @BeforeEach
  public void setUp() throws SQLException {
    final var dataSource = this.mySqlDataSource();

    new JdbcSession(dataSource).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue")
        .execute();

    new JdbcSession(dataSource).sql(
        "CREATE TABLE ar_cpfw_jqueue ( " + "id int NOT NULL auto_increment,  "
            + "channel varchar(100) NOT NULL, " + "data text NOT NULL, "
            + "attempt int, " + "delay int, " + "pushed_at timestamp, "
            + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  void pushIsRolledBackIfTxFails() throws SQLException {
    new PushUseCases(mySqlDataSource()).pushIsRolledBackIfTxFails();
  }

  @Test
  void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new PushUseCases(mySqlDataSource()).pushCanBeDoneBySpecifyingAChannel();
  }

  @Test
  void pushCanBeDoneInDefaultChannel() throws SQLException {
    new PushUseCases(mySqlDataSource()).pushCanBeDoneInDefaultChannel();
  }


  private DataSource mySqlDataSource() {
    final MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(this.container.getJdbcUrl());
    dataSource.setUser(this.container.getUsername());
    dataSource.setPassword(this.container.getPassword());
    return dataSource;
  }
}
