package ar.cpfw.jqueue.push;

import java.sql.Connection;

public interface JTxQueue {

    static JTxQueue queue(Connection currentInTxConnection, String tableName) {
        return new JdbcJQueue(currentInTxConnection, tableName);
    }

    static JTxQueue queue(Connection currentInTxConnection) {
        return new JdbcJQueue(currentInTxConnection, null);
    }

    void push(String data);

    JTxQueue channel(String channelName);

}
