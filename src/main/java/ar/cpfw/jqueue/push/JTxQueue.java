package ar.cpfw.jqueue.push;

import java.sql.Connection;
import javax.sql.DataSource;

public interface JTxQueue {

  void push(String data);

  JTxQueue channel(String channelName);

  static JTxQueue queue(DataSource dataSource, String tableName) {
    return new JdbcJQueue(dataSource, tableName);
  }

  static JTxQueue queue(Connection conn, String tableName) {
    return new JdbcJQueue(conn, tableName);
  }

  static JTxQueue queue(DataSource dataSource) {
    return new JdbcJQueue(dataSource, null);
  }

  static JTxQueue queue(Connection conn) {
    return new JdbcJQueue(conn, null);
  }

}
