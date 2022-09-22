package ar.cpfw.jqueue.push;

import java.sql.Connection;

/**
 * JTxQueue.
 *
 * <p>
 * Push some data into the black channel:
 * </p>
 * 
 * <pre>
 * JTxQueue.queue(Connection).channel("black").push("Hello World!");
 * </pre>
 *
 * <p>
 * Push some data into the default channel:
 * </p>
 *
 * <pre>
 * JTxQueue.queue(Connection).push("Hello World!");
 * </pre>
 *
 * @since 0.1
 */
public interface JTxQueue {

  void push(String data);

  JTxQueue channel(String channelName);

  static JTxQueue queue(Connection c) {
    return new JdbcJQueue(c);
  }

}
