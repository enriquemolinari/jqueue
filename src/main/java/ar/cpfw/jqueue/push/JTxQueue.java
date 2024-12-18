package ar.cpfw.jqueue.push;

import javax.sql.DataSource;
import java.sql.Connection;

public interface JTxQueue {

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

    void push(String data);

    JTxQueue channel(String channelName);

}
