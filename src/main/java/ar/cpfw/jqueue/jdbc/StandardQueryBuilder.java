package ar.cpfw.jqueue.jdbc;

import ar.cpfw.jqueue.runner.QueryBuilder;

public abstract class StandardQueryBuilder implements QueryBuilder {

  private String QUEUE_TABLE_NAME = "ar_cpfw_jqueue";

  @Override
  public String readQuery() {
    // important: the "order by" must be by the PK if you want to lock only 1 row
    // I'm using a timeBased UUID
    // https://github.com/cowtowncoder/java-uuid-generator
    // to make this work
    return "select id, data, attempt from " + QUEUE_TABLE_NAME + ""
        + " where channel = ? and pushed_at <= " + calculateDate() + " order by id asc " + limitOne()
        + " for update skip locked";
  }

  @Override
  public String updateQueryOnFail() {
    return "update " + QUEUE_TABLE_NAME + " set attempt = ?, delay = ? where id = ?";
  }

  @Override
  public String deleteQueryOnSuccess() {
    return "delete from " + QUEUE_TABLE_NAME + " where id = ?";
  }

  protected abstract String calculateDate();

  protected abstract String limitOne();

}
