package ar.cpfw.jqueue.push;

import com.jcabi.jdbc.JdbcSession;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushTableNameHsqlDbTest {

    @BeforeEach
    void setUp() throws SQLException {
        var dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
        dataSource.setUser("SA");
        dataSource.setPassword("");

        new JdbcSession(dataSource).sql("DROP SCHEMA PUBLIC CASCADE").execute();
    }

    @Test
    void pushWorksWithTableName() throws SQLException {
        final var ds = hSqlDataSource();
        new JdbcSession(ds)
                .sql("CREATE TABLE if not exists schema1jqueuetable ( "
                        + "id int NOT NULL IDENTITY,  " + "channel varchar(100) NOT NULL, "
                        + "data text NOT NULL, " + "attempt int, " + "delay int, "
                        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
                .execute();

        final var conn = ds.getConnection();
        try {
            final var queue = JTxQueue.queue(conn, "schema1jqueuetable");
            queue.channel("anotherChannel").push("Hola Mundo!");

        } finally {
            conn.close();
        }

        int totalRows =
                new JdbcSession(ds).sql("select count(*) from schema1jqueuetable")
                        .select((rset, stmt) -> {
                            rset.next();
                            return rset.getInt(1);
                        });

        assertEquals(1, totalRows);

        Map<String, String> outcome = new JdbcSession(ds)
                .sql("select channel, data, attempt, delay from schema1jqueuetable")
                .select((rset, stmt) -> {

                    rset.next();
                    return Map.of("channel", rset.getString(1), "data",
                            rset.getString(2), "attempt", String.valueOf(rset.getInt(3)),
                            "delay", String.valueOf(rset.getInt(4)));
                });

        assertEquals("anotherChannel", outcome.get("channel"));
        assertEquals("Hola Mundo!", outcome.get("data"));
        assertEquals("0", outcome.get("attempt"));
        assertEquals("0", outcome.get("delay"));
    }

    @Test
    void pushWorksWithTableNameAndDataSource() throws SQLException {
        var ds = hSqlDataSource();
        new JdbcSession(ds)
                .sql("CREATE TABLE if not exists schema1jqueuetable ( "
                        + "id int IDENTITY,  " + "channel varchar(100) NOT NULL, "
                        + "data text NOT NULL, " + "attempt int, " + "delay int, "
                        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
                .execute();

        try {
            var queue = JTxQueue.queue(ds, "schema1jqueuetable");
            queue.channel("anotherChannel").push("Hola Mundo!");

        } finally {
            ds.getConnection().close();
        }

        int totalRows =
                new JdbcSession(ds).sql("select count(*) from schema1jqueuetable")
                        .select((rset, stmt) -> {
                            rset.next();
                            return rset.getInt(1);
                        });

        assertEquals(1, totalRows);

        Map<String, String> outcome = new JdbcSession(ds)
                .sql("select channel, data, attempt, delay from schema1jqueuetable")
                .select((rset, stmt) -> {

                    rset.next();
                    return Map.of("channel", rset.getString(1), "data",
                            rset.getString(2), "attempt", String.valueOf(rset.getInt(3)),
                            "delay", String.valueOf(rset.getInt(4)));
                });

        assertEquals("anotherChannel", outcome.get("channel"));
        assertEquals("Hola Mundo!", outcome.get("data"));
        assertEquals("0", outcome.get("attempt"));
        assertEquals("0", outcome.get("delay"));
    }

    private DataSource hSqlDataSource() {
        final var dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true");
        dataSource.setUser("SA");
        dataSource.setPassword("");
        return dataSource;
    }
}
