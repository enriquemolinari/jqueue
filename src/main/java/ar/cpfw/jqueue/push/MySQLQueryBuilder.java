package ar.cpfw.jqueue.push;

class MySQLQueryBuilder extends StandardQueryBuilder {

  @Override
  protected String calculateDate() {
    return "date_sub(?, interval delay minute)";
  }

  @Override
  protected String limitOne() {
    return "limit 1";
  }

}