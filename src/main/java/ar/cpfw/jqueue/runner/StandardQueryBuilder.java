package ar.cpfw.jqueue.runner;

import java.util.Objects;

abstract class StandardQueryBuilder implements QueryBuilder {

  private String tableName;

  public StandardQueryBuilder(final String tableName) {
    Objects.requireNonNull(tableName,
        "A database table name must be specified");
    this.tableName = tableName;
  }

  @Override
  public String readQuery() {
    // important: the "order by" must be by the PK if you want to lock only 1 row
    // I'm using a timeBased UUID
    // https://github.com/cowtowncoder/java-uuid-generator
    // to make this work
    return "select id, data, attempt from " + this.tableName + ""
        + " where channel = ? and pushed_at <= " + calculateDate()
        + " order by id asc " + limitOne() + " " + lock();
  }

  protected String lock() {
    return "for update skip locked";
  }

  @Override
  public String updateQueryOnFail() {
    return "update " + this.tableName
        + " set attempt = ?, delay = ? where id = ?";
  }

  @Override
  public String deleteQueryOnSuccess() {
    return "delete from " + this.tableName + " where id = ?";
  }

  protected abstract String calculateDate();

  protected abstract String limitOne();

}
