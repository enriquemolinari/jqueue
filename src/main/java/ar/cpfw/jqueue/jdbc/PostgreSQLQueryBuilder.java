package ar.cpfw.jqueue.jdbc;

public class PostgreSQLQueryBuilder extends StandardQueryBuilder {

  @Override
  protected String calculateDate() {
    return "(timestamp ? - (INTERVAL '1 minute' * delay))";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

}
