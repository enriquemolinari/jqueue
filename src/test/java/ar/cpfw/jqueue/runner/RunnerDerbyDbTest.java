package ar.cpfw.jqueue.runner;

import ar.cpfw.jqueue.push.PushDerbyDbTest;
import com.jcabi.jdbc.JdbcSession;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RunnerDerbyDbTest {
    public static final String JDBC_DERBYDB = "memory:testdb";
    public static final String JDBC_DERBYDB_DM = "jdbc:derby:" + JDBC_DERBYDB;
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

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void runnerWorksWithOneJob(RunnerUseCases useCases) throws SQLException {
        useCases.runnerWorksWithOneJob(derbyDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void failJobIsNotExecutedYet(RunnerUseCases useCases) throws SQLException {
        useCases.failJobIsNotExecutedYet(derbyDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void jobThatFailsIsPushedBack(RunnerUseCases useCases) throws SQLException {
        useCases.jobThatFailsIsPushedBack(derbyDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void runnerWorksWithTwoJobs(RunnerUseCases useCases) throws SQLException {
        useCases.runnerWorksWithTwoJobs(derbyDataSource());
    }

    Stream<Arguments> provideRunnerInstances() {
        return Stream.of(
                Arguments.of(new RunnerUseCases(derbyDataSource())),
                Arguments.of(new RunnerUseCases(JDBC_DERBYDB_DM, USER, PWD))
        );
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
