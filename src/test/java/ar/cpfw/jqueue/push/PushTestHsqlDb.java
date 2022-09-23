package ar.cpfw.jqueue.push;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class PushTestHsqlDb {

  private Connection getConn() throws SQLException {
    return DriverManager
        .getConnection("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true", "SA", "");
  }

  // TODO: Test que el push esta en una Tx con una conn iniciada...
  // Cambiar a DataSource
  // Ver como mejorar si uso esquema o no...

  @Test
  public void test() throws SQLException {

    var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
    ds.setUser("SA");
    ds.setPassword("");

    new JdbcSession(ds).sql("CREATE TABLE ar_cpfw_jqueue ( "
        + "id char(36) NOT NULL,  " + "channel varchar(100) NOT NULL, "
        + "data text NOT NULL, " + "attempt int, " + "delay int, "
        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
        .execute();

    var queue = JTxQueue.queue(ds.getConnection());
    queue.push("Hola Mundo!");

    String s = new JdbcSession(ds).sql("select data from ar_cpfw_jqueue")
        .select(new Outcome<String>() {
          @Override
          public String handle(final ResultSet rset, final Statement stmt)
              throws SQLException {
            rset.next();
            return rset.getString(1);
          }
        });

    System.out.println(s);
  }
}
