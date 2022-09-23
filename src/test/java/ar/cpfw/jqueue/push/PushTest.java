package ar.cpfw.jqueue.push;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
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
public class PushTest {

  /**
   * The database container.
   */
  @Container
  private final JdbcDatabaseContainer<?> container =
      new MySQLContainer<>(DockerImageName.parse("mysql/mysql-server:latest")
          .asCompatibleSubstituteFor("mysql"));

  @Test
  public void test1() throws SQLException {
    try (Connection conn = this.source().getConnection()) {
      Statement stmt = conn.createStatement();

      String sql = "CREATE TABLE ar_cpfw_jqueue ( " + "id char(36) NOT NULL,  "
          + "channel varchar(100) NOT NULL, " + "data text NOT NULL, "
          + "attempt int, " + "delay int, " + "pushed_at timestamp, "
          + "CONSTRAINT id_pk PRIMARY KEY (id));";

      stmt.executeUpdate(sql);
    }
  }

  private DataSource source() {
    final MysqlDataSource src = new MysqlDataSource();
    src.setUrl(this.container.getJdbcUrl());
    src.setUser(this.container.getUsername());
    src.setPassword(this.container.getPassword());
    return src;
  }
}
