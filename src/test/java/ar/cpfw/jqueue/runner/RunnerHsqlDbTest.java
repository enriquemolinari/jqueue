package ar.cpfw.jqueue.runner;

import com.jcabi.jdbc.JdbcSession;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RunnerHsqlDbTest {

    public static final String URL = "jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true";
    public static final String USER = "SA";
    public static final String PWD = "";

    @BeforeEach
    void setUp() throws SQLException {
        final var dataSource = hSqlDataSource();

        new JdbcSession(dataSource).sql("DROP SCHEMA PUBLIC CASCADE").execute();

        new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
                        + "id int NOT NULL IDENTITY,  " + "channel varchar(100) NOT NULL, "
                        + "data text NOT NULL, " + "attempt int, " + "delay int, "
                        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
                .execute();
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void runnerWorksWithOneJob(RunnerUseCases useCases) throws SQLException {
        useCases.runnerWorksWithOneJob(hSqlDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void failJobIsNotExecutedYet(RunnerUseCases useCases) throws SQLException {
        useCases.failJobIsNotExecutedYet(hSqlDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void jobThatFailsIsPushedBack(RunnerUseCases useCases) throws SQLException {
        useCases.jobThatFailsIsPushedBack(hSqlDataSource());
    }

    @ParameterizedTest
    @MethodSource("provideRunnerInstances")
    void runnerWorksWithTwoJobs(RunnerUseCases useCases) throws SQLException {
        useCases.runnerWorksWithTwoJobs(hSqlDataSource());
    }

    Stream<Arguments> provideRunnerInstances() {
        return Stream.of(
                Arguments.of(new RunnerUseCases(hSqlDataSource())),
                Arguments.of(new RunnerUseCases(URL, USER, PWD))
        );
    }

    private DataSource hSqlDataSource() {
        final var dataSource = new JDBCDataSource();
        dataSource.setUrl(URL);
        dataSource.setUser(USER);
        dataSource.setPassword(PWD);
        return dataSource;
    }
}
