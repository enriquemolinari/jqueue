package ar.cpfw.jqueue.runner;

class DerbyDbQueryBuilder extends StandardQueryBuilder {

    public DerbyDbQueryBuilder(final String tableName) {
        super(tableName);
    }

    @Override
    protected String calculateDate() {
        return "{fn TIMESTAMPADD(SQL_TSI_MINUTE, -delay, ?)}";
    }

    @Override
    protected String limitOne() {
        return "FETCH FIRST 1 ROWS ONLY";
    }

    @Override
    protected String lock() {
        //as of version 10.16.1.1
        // Derby does not support for update with an order by
        return "";
    }
}
