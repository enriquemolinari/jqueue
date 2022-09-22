package ar.cpfw.jqueue.runner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import ar.cpfw.jqueue.JQueueException;

interface QueryBuilder {
  String readQuery();

  String updateQueryOnFail();

  String deleteQueryOnSuccess();

  static QueryBuilder build(Connection conn) {
    var map = Map.of("MySQL", new MySQLQueryBuilder(), "PostgreSQL", new PostgreSqlQueryBuilder());

    try {
      var qb = map.get(conn.getMetaData().getDatabaseProductName());
      if (qb == null) {
        throw new JQueueException("Your database vendor is not supported");
      }

      return qb;
    } catch (SQLException e) {
      throw new JQueueException(e, "Your database vendor name could not be retrieved");
    }

  }
}
