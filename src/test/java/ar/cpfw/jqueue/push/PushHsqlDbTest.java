package ar.cpfw.jqueue.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.PreparedStatement;
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
  public void queuePushIsRollbackIfTxFailTest() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds).autocommit(false)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute().sql("CREATE TABLE if not exists test (name VARCHAR(50))")
        .execute().commit();

    var conn = ds.getConnection();
    try {
      conn.setAutoCommit(false);

      var queue = JTxQueue.queue(conn);
      queue.push("Hola Mundo!");

      PreparedStatement st = conn.prepareStatement(
          "insert into table_does_not_exists (name) values (?)");
      st.setString(1, "any name");
      st.executeUpdate();
      conn.commit();

    } catch (Exception e) {
      conn.rollback();
    } finally {
      conn.setAutoCommit(true);
      conn.close();
    }


    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
            .select(new Outcome<Integer>() {
              @Override
              public Integer handle(final ResultSet rset, final Statement stmt)
                  throws SQLException {
                rset.next();
                return rset.getInt(1);
              }
            });

    assertEquals(0, totalRows);

  }

  @Test
  public void queuePushCanBeDoneInSpecificChannelTest() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var conn = ds.getConnection();
    try {
      var queue = JTxQueue.queue(conn);
      queue.channel("anotherChannel").push("Hola Mundo!");

    } finally {
      conn.close();
    }

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
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
        .sql("select channel, data, attempt, delay from ar_cpfw_jqueue")
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

  @Test
  public void queuePushCanBeDoneInDefaultChannelTest() throws SQLException {
    var ds = hSqlDataSource();

    new JdbcSession(ds)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var conn = ds.getConnection();
    try {
      var queue = JTxQueue.queue(conn);
      queue.push("Hola Mundo!");

    } finally {
      conn.close();
    }

    int totalRows =
        new JdbcSession(ds).sql("select count(*) from ar_cpfw_jqueue")
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
        .sql("select channel, data, attempt, delay from ar_cpfw_jqueue")
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

    assertEquals("default", outcome.get("channel"));
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
