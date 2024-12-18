package ar.cpfw.jqueue.push;

import com.jcabi.jdbc.JdbcSession;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

public class PushHsqlDbTest {

    private static final String JDBC_HSQLDB = "jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true";
    private static final String USER = "SA";
    private static final String PWD = "";

    @BeforeEach
    void setUp() throws SQLException {
        var dataSource = hSqlDataSource();

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
        dataSource.setUrl(JDBC_HSQLDB);
        dataSource.setUser(USER);
        dataSource.setPassword(PWD);
        return dataSource;
    }
}
