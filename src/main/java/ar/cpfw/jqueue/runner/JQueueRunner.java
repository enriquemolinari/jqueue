package ar.cpfw.jqueue.runner;

public interface JQueueRunner {
  void executeAll(Job job);

  JQueueRunner channel(String channelName);

  static JQueueRunner runner(String connStr, String user, String pwd) {
    return new JdbcJQueueRunner(connStr, user, pwd);
  }
}
