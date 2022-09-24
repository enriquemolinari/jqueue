package ar.cpfw.jqueue.runner;

class MySQLQueryBuilder extends StandardQueryBuilder {

  public MySQLQueryBuilder(String tableName) {
    super(tableName);
  }

  @Override
  protected String calculateDate() {
    return "date_sub(?, interval delay minute)";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

}
