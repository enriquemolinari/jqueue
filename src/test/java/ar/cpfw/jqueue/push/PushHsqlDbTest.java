package ar.cpfw.jqueue.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class PushHsqlDbTest {

  @BeforeEach
  public void setUp() throws SQLException {
    var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    ds.setUser("SA");
    ds.setPassword("");

    new JdbcSession(ds).sql("DROP SCHEMA PUBLIC CASCADE").execute();
  }

  @Test
  public void pushIsRolledBackIfTxFails() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushIsRolledBackIfTxFails();
  }

  @Test
  public void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushCanBeDoneBySpecifyingAChannel();
  }

  @Test
  public void pushCanBeDoneInDefaultChannel() throws SQLException {
    new PushUseCases(hSqlDataSource()).pushCanBeDoneInDefaultChannel();
  }

  @Test
  public void bla() throws SQLException {
    var ds = hSqlDataSource();
    new JdbcSession(ds)
        .sql("CREATE TABLE if not exists schema1jqueuetable ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var conn = ds.getConnection();
    try {
      var queue = JTxQueue.queue(conn, "schema1jqueuetable");
      queue.channel("anotherChannel").push("Hola Mundo!");

    } finally {
      conn.close();
    }

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from schema1jqueuetable")
            .select(new Outcome<Integer>() {
              @Override
              public Integer handle(final ResultSet rset, final Statement stmt)
                  throws SQLException {
                rset.next();
                return rset.getInt(1);
              }
            });

    assertEquals(1, totalRows);

    Map<String, String> outcome = new JdbcSession(ds)
        .sql("select channel, data, attempt, delay from schema1jqueuetable")
        .select(new Outcome<Map<String, String>>() {
          @Override
          public Map<String, String> handle(final ResultSet rset,
              final Statement stmt) throws SQLException {

            rset.next();
            return Map.of("channel", rset.getString(1), "data",
                rset.getString(2), "attempt", String.valueOf(rset.getInt(3)),
                "delay", String.valueOf(rset.getInt(4)));
          }
        });

    assertEquals("anotherChannel", outcome.get("channel"));
    assertEquals("Hola Mundo!", outcome.get("data"));
    assertEquals("0", outcome.get("attempt"));
    assertEquals("0", outcome.get("delay"));
  }

  private DataSource hSqlDataSource() {
    final var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    ds.setUser("SA");
    ds.setPassword("");
    return ds;
  }
}
