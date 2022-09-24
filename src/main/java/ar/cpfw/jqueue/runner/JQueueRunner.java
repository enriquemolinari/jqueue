package ar.cpfw.jqueue.runner;

import javax.sql.DataSource;

public interface JQueueRunner {
  void executeAll(Job job);

  JQueueRunner channel(String channelName);

  static JQueueRunner runner(DataSource dataSource, String tableName) {
    return new JdbcJQueueRunner(dataSource, tableName);
  }

  static JQueueRunner runner(DataSource dataSource) {
    return new JdbcJQueueRunner(dataSource, null);
  }
}
