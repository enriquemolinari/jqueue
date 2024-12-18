package ar.cpfw.jqueue.runner;

import com.jcabi.jdbc.JdbcSession;
import com.mysql.cj.jdbc.MysqlDataSource;
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
        final var dataSource = this.source();

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
        new RunnerUseCases(source()).runnerWorksWithOneJob();
    }

    @Test
    void failJobIsNotExecutedYet() throws SQLException {
        new RunnerUseCases(source()).failJobIsNotExecutedYet();
    }

    @Test
    void jobThatFailsIsPushedBack() throws SQLException {
        new RunnerUseCases(source()).jobThatFailsIsPushedBack();
    }

    @Test
    void runnerWorksWithTwoJobs() throws SQLException {
        new RunnerUseCases(source()).runnerWorksWithTwoJobs();
    }

    private DataSource source() {
        final MysqlDataSource src = new MysqlDataSource();
        src.setUrl(this.container.getJdbcUrl());
        src.setUser(this.container.getUsername());
        src.setPassword(this.container.getPassword());
        return src;
    }
}
