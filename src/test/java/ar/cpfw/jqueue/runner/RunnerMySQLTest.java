package ar.cpfw.jqueue.runner;

import com.jcabi.jdbc.JdbcSession;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * If you run on Ubuntu, make sure your user can run docker, if not:
 *
 * <pre>
 *  sudo groupadd docker sudo
 *  sudo gpasswd -a $USER docker
 *  then restart your PC
 * </pre>
 */
@Testcontainers(disabledWithoutDocker = true)
public class RunnerMySQLTest {
    /**
     * The database container.
     */
    @Container
    private final JdbcDatabaseContainer<?> container = new MySQLContainer<>(
            DockerImageName.parse("mysql:latest").asCompatibleSubstituteFor("mysql"));

    @BeforeEach
    void setUp() throws SQLException {
        final var dataSource = this.mySqlSource();

        new JdbcSession(dataSource).sql("DROP TABLE IF EXISTS ar_cpfw_jqueue")
                .execute();

        new JdbcSession(dataSource).sql(
                        "CREATE TABLE ar_cpfw_jqueue ( " + "id int NOT NULL auto_increment,  "
                                + "channel varchar(100) NOT NULL, " + "data text NOT NULL, "
                                + "attempt int, " + "delay int, " + "pushed_at timestamp, "
                                + "CONSTRAINT id_pk PRIMARY KEY (id));")
                .execute();
    }

    @Test
    void runnerWorksWithOneJob() throws SQLException {
        getWithDataSourceConstructor().runnerWorksWithOneJob(mySqlSource());
    }

    @Test
    void runnerWorksWithOneJobDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().runnerWorksWithOneJob(mySqlSource());
    }

    @Test
    void failJobIsNotExecutedYet() throws SQLException {
        getWithDataSourceConstructor().failJobIsNotExecutedYet(mySqlSource());
    }

    @Test
    void failJobIsNotExecutedYetDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().failJobIsNotExecutedYet(mySqlSource());
    }

    @Test
    void jobThatFailsIsPushedBack() throws SQLException {
        getWithDataSourceConstructor().jobThatFailsIsPushedBack(mySqlSource());
    }

    @Test
    void jobThatFailsIsPushedBackDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().jobThatFailsIsPushedBack(mySqlSource());
    }

    @Test
    void runnerWorksWithTwoJobs() throws SQLException {
        getWithDataSourceConstructor().runnerWorksWithTwoJobs(mySqlSource());
    }

    @Test
    void runnerWorksWithTwoJobsDMConstructor() throws SQLException {
        getWithDriverManagerConstructor().runnerWorksWithTwoJobs(mySqlSource());
    }

    private @NotNull RunnerUseCases getWithDataSourceConstructor() {
        return new RunnerUseCases(mySqlSource());
    }

    private @NotNull RunnerUseCases getWithDriverManagerConstructor() {
        return new RunnerUseCases(this.container.getJdbcUrl(), this.container.getUsername(), this.container.getPassword());
    }

    private DataSource mySqlSource() {
        final MysqlDataSource src = new MysqlDataSource();
        src.setUrl(this.container.getJdbcUrl());
        src.setUser(this.container.getUsername());
        src.setPassword(this.container.getPassword());
        return src;
    }
}
