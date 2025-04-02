package ar.cpfw.jqueue.runner;

import com.jcabi.jdbc.JdbcSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.SQLException;

@Testcontainers(disabledWithoutDocker = true)
public class RunnerPgTest {

    /**
     * The database container.
     */
    @Container
    private final JdbcDatabaseContainer<?> container =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest")
                    .asCompatibleSubstituteFor("postgres"));

    @BeforeEach
    void setUp() throws SQLException {
//        if (!container.isCreated())
//            container.start();
        final var dataSource = this.pgSource();

        new JdbcSession(dataSource).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue")
                .execute();

        new JdbcSession(dataSource).sql("CREATE TABLE ar_cpfw_jqueue ( "
                        + "id serial,  " + "channel varchar(100) NOT NULL, "
                        + "data text NOT NULL, " + "attempt int, " + "delay int, "
                        + "pushed_at timestamp, " + "CONSTRAINT id_pk PRIMARY KEY (id));")
                .execute();
    }

    @Test
    void runnerWorksWithOneJob() throws SQLException {
        getWithDataSourceConstructor().runnerWorksWithOneJob(pgSource());
    }

    @Test
    void runnerWorksWithOneJobDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().runnerWorksWithOneJob(pgSource());
    }

    private @NotNull RunnerUseCases getWithDataSourceConstructor() {
        return new RunnerUseCases(pgSource());
    }

    private @NotNull RunnerUseCases getWithDriverManagerConstructor() {
        return new RunnerUseCases(this.container.getJdbcUrl(), this.container.getUsername(), this.container.getPassword());
    }

    @Test
    void failJobIsNotExecutedYet() throws SQLException {
        getWithDataSourceConstructor().failJobIsNotExecutedYet(pgSource());
    }

    @Test
    void failJobIsNotExecutedYetDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().failJobIsNotExecutedYet(pgSource());
    }

    @Test
    void jobThatFailsIsPushedBack() throws SQLException {
        getWithDataSourceConstructor().jobThatFailsIsPushedBack(pgSource());
    }

    @Test
    void jobThatFailsIsPushedBackDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().jobThatFailsIsPushedBack(pgSource());
    }

    @Test
    void runnerWorksWithTwoJobs() throws SQLException {
        getWithDataSourceConstructor().runnerWorksWithTwoJobs(pgSource());
    }

    @Test
    void runnerWorksWithTwoJobsDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().runnerWorksWithTwoJobs(pgSource());
    }

    private DataSource pgSource() {
        final PGSimpleDataSource src = new PGSimpleDataSource();
        src.setUrl(this.container.getJdbcUrl());
        src.setUser(this.container.getUsername());
        src.setPassword(this.container.getPassword());
        return src;
    }
}
