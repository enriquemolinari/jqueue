package ar.cpfw.jqueue.runner;

import ar.cpfw.jqueue.push.PushDerbyDbTest;
import com.jcabi.jdbc.JdbcSession;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

public class RunnerDerbyDbTest {
    public static final String JDBC_DERBYDB = "memory:testdb";
    public static final String USER = "app";
    public static final String PWD = "app";

    @BeforeEach
    void setUp() throws SQLException {
        final var dataSource = derbyDataSource();

        try {
            new JdbcSession(dataSource).sql("drop table " + PushDerbyDbTest.JQUEUE_TABLE_NAME).execute();
        } catch (Exception e) {
            //do nothing if the table does not exists
        }

        new JdbcSession(dataSource).sql(PushDerbyDbTest.DERBY_CREATE_TABLE_STMT)
                .execute();
    }

    @Test
    void runnerWorksWithOneJob() throws SQLException {
        new RunnerUseCases(derbyDataSource()).runnerWorksWithOneJob();
    }

    @Test
    void failJobIsNotExecutedYet() throws SQLException {
        new RunnerUseCases(derbyDataSource()).failJobIsNotExecutedYet();
    }

    @Test
    void jobThatFailsIsPushedBack() throws SQLException {
        new RunnerUseCases(derbyDataSource()).jobThatFailsIsPushedBack();
    }

    @Test
    void runnerWorksWithTwoJobs() throws SQLException {
        new RunnerUseCases(derbyDataSource()).runnerWorksWithTwoJobs();
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
