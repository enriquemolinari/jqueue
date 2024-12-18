package ar.cpfw.jqueue.runner;

import ar.cpfw.jqueue.JQueueException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

interface QueryBuilder {
    String POSTGRE_SQL = "PostgreSQL";
    String MY_SQL = "MySQL";
    String HSQLDB = "HSQL Database Engine";
    String DERBYDB = "Apache Derby";

    static QueryBuilder build(final Connection conn, final String tableName) {
        final var map = Map.of(MY_SQL, new MySQLQueryBuilder(tableName),
                POSTGRE_SQL, new PostgreSqlQueryBuilder(tableName), HSQLDB,
                new HsqlDbQueryBuilder(tableName),
                DERBYDB, new DerbyDbQueryBuilder(tableName));

        try {
            final var qb = map.get(conn.getMetaData().getDatabaseProductName());
            if (qb == null) {
                throw new JQueueException("Your database vendor is not supported");
            }

            return qb;
        } catch (SQLException e) {
            throw new JQueueException(e,
                    "Your database vendor name could not be retrieved");
        }
    }

    String readQuery();

    String updateQueryOnFail();

    String deleteQueryOnSuccess();
}
