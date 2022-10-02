package ar.cpfw.jqueue.push;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.jcabi.jdbc.JdbcSession;

public class PushHsqlDbTest {

  @BeforeEach
  void setUp() throws SQLException {
    var dataSource = new JDBCDataSource();
    dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    dataSource.setUser("SA");
    dataSource.setPassword("");

    new JdbcSession(dataSource).sql("DROP SCHEMA PUBLIC CASCADE").execute();

    new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id int IDENTITY,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();
  }

  @Test
  void pushIsRolledBackIfTxFails() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushIsRolledBackIfTxFails();
  }

  @Test
  void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushCanBeDoneBySpecifyingAChannel();
  }

  @Test
  void pushCanBeDoneInDefaultChannel() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushCanBeDoneInDefaultChannel();
  }

  private DataSource hSqlDataSource() {
    final var dataSource = new JDBCDataSource();
    dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    dataSource.setUser("SA");
    dataSource.setPassword("");
    return dataSource;
  }
}
