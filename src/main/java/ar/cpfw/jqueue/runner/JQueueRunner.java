package ar.cpfw.jqueue.runner;

import ar.cpfw.jqueue.jdbc.JdbcJQueueRunner;
import ar.cpfw.jqueue.push.Job;

public interface JQueueRunner {
  void executeAll(Job job);

  JQueueRunner channel(String channelName);

  static JQueueRunner runner(String connStr, String user, String pwd) {
    return new JdbcJQueueRunner(connStr, user, pwd);
  }
}
