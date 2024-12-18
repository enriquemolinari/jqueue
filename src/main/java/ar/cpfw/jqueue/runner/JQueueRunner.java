package ar.cpfw.jqueue.runner;

import javax.sql.DataSource;

public interface JQueueRunner {
    static JQueueRunner runner(DataSource dataSource, String tableName) {
        return new JdbcJQueueRunner(dataSource, tableName);
    }

    static JQueueRunner runner(DataSource dataSource) {
        return new JdbcJQueueRunner(dataSource, null);
    }

    void executeAll(Job job);

    JQueueRunner channel(String channelName);
}
