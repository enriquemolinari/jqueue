package ar.cpfw.jqueue.runner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import ar.cpfw.jqueue.JQueueException;

interface QueryBuilder {
  static final String POSTGRE_SQL = "PostgreSQL";
  static final String MY_SQL = "MySQL";

  String readQuery();

  String updateQueryOnFail();

  String deleteQueryOnSuccess();

  static QueryBuilder build(final Connection conn) {
    final var map = Map.of(MY_SQL, new MySQLQueryBuilder(), POSTGRE_SQL,
        new PostgreSqlQueryBuilder());

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
}
