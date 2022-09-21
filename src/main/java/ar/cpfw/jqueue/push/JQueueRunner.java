package ar.cpfw.jqueue.push;

public interface JQueueRunner {
  void executeAll(Job job);

  JQueueRunner channel(String channelName);

  static JQueueRunner runner(String connStr, String user, String pwd) {
    return new JdbcJQueueRunner(connStr, user, pwd);
  }
}
