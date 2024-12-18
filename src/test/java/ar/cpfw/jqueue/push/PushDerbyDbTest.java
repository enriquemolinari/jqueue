package ar.cpfw.jqueue.push;

import com.jcabi.jdbc.JdbcSession;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

public class PushDerbyDbTest {

    public static final String JDBC_DERBYDB = "memory:testdb";
    public static final String USER = "app";
    public static final String PWD = "app";
    public static final String JQUEUE_TABLE_NAME = "ar_cpfw_jqueue";
    public static final String DERBY_CREATE_TABLE_STMT = "CREATE TABLE " + JQUEUE_TABLE_NAME + " ( "
            + "id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),  "
            + "channel varchar(100) NOT NULL, "
            + "data CLOB NOT NULL, " + "attempt int, " + "delay int, "
            + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id))";

    @BeforeEach
    void setUp() throws SQLException {
        var dataSource = derbyDataSource();

        try {
            new JdbcSession(dataSource).sql("drop table " + JQUEUE_TABLE_NAME).execute();
        } catch (Exception e) {
            //do nothing if the table does not exists
        }

        new JdbcSession(dataSource).sql(DERBY_CREATE_TABLE_STMT)
                .execute();
    }

    @Test
    void pushIsRolledBackIfTxFails() throws SQLException {
        new PushUseCases(derbyDataSource()).pushIsRolledBackIfTxFails();
    }

    @Test
    void pushCanBeDoneBySpecifyingAChannel() throws SQLException {
        new PushUseCases(derbyDataSource()).pushCanBeDoneBySpecifyingAChannel();
    }

    @Test
    void pushCanBeDoneInDefaultChannel() throws SQLException {
        new PushUseCases(derbyDataSource()).pushCanBeDoneInDefaultChannel();
    }

    private DataSource derbyDataSource() {
        final var dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName(JDBC_DERBYDB);
        dataSource.setCreateDatabase("create");
        dataSource.setUser(USER);
        dataSource.setPassword(PWD);
        return dataSource;
    }
}
