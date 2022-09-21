package ar.cpfw.jqueue.push;

class PostgreSqlQueryBuilder extends StandardQueryBuilder {

  @Override
  protected String calculateDate() {
    return "(CAST (? as TIMESTAMP) - (INTERVAL '1 minute' * delay))";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

}
