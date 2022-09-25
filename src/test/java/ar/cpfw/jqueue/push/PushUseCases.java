package ar.cpfw.jqueue.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class PushUseCases {

  private DataSource dataSource;

  public PushUseCases(final DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void pushIsRolledBackIfTxFails() throws SQLException {
    new JdbcSession(this.dataSource).autocommit(false)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute().sql("CREATE TABLE if not exists test (name VARCHAR(50))")
        .execute().commit();

    final var conn = this.dataSource.getConnection();
    try {
      conn.setAutoCommit(false);

      final var queue = JTxQueue.queue(conn);
      queue.push("Hola Mundo!");

      // by purpuse I use a table that doesn't exists to make the Tx to fail
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

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
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

  public void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
    new JdbcSession(this.dataSource)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var conn = this.dataSource.getConnection();
    try {
      var queue = JTxQueue.queue(conn);
      queue.channel("anotherChannel").push("Hola Mundo!");

    } finally {
      conn.close();
    }

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
        .select(new Outcome<Integer>() {
          @Override
          public Integer handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getInt(1);
          }
        });

    assertEquals(1, totalRows);

    Map<String, String> outcome = new JdbcSession(this.dataSource)
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

  public void pushCanBeDoneInDefaultChannel() throws SQLException {
    new JdbcSession(this.dataSource)
        .sql("CREATE TABLE if not exists ar_cpfw_jqueue ( "
            + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
            + "data text NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var conn = this.dataSource.getConnection();
    try {
      var queue = JTxQueue.queue(conn);
      queue.push("Hola Mundo!");

    } finally {
      conn.close();
    }

    final int totalRows = new JdbcSession(this.dataSource)
        .sql("select count(*) from ar_cpfw_jqueue")
        .select(new Outcome<Integer>() {
          @Override
          public Integer handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getInt(1);
          }
        });

    assertEquals(1, totalRows);

    Map<String, String> outcome = new JdbcSession(this.dataSource)
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
}
