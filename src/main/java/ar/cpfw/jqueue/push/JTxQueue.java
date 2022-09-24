package ar.cpfw.jqueue.push;

import java.sql.Connection;
import javax.sql.DataSource;

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

  static JTxQueue queue(DataSource dataSource) {
    return new JdbcJQueue(dataSource);
  }

  static JTxQueue queue(Connection conn) {
    return new JdbcJQueue(conn);
  }

}
