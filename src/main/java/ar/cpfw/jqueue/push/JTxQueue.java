package ar.cpfw.jqueue.push;

import java.sql.Connection;

public interface JTxQueue {

  void push(String data);

  JTxQueue channel(String channelName);

  static JTxQueue queue(Connection c) {
    return new JdbcJQueue(c);
  }

}
