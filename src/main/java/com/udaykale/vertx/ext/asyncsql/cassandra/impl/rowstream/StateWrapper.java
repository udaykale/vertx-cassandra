package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

/**
 * @param <T>
 * @author uday
 */
interface StateWrapper<T extends StateWrapper> {

    State<T> getState();

    void setState(State<T> state);
}
