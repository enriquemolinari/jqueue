package ar.cpfw.jqueue.runner;

class PostgreSqlQueryBuilder extends StandardQueryBuilder {

  public PostgreSqlQueryBuilder(final String tableName) {
    super(tableName);
  }

  @Override
  protected String calculateDate() {
    return "(CAST (? as TIMESTAMP) - (INTERVAL '1 minute' * delay))";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

}
