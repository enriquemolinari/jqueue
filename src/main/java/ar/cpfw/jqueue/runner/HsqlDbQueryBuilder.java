package ar.cpfw.jqueue.runner;

class HsqlDbQueryBuilder extends StandardQueryBuilder {

  @Override
  protected String calculateDate() {
    return "(CAST (? as TIMESTAMP) - (INTERVAL '1' MINUTE * delay))";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

  @Override
  protected String lock() {
    return "for update";
  }
}
