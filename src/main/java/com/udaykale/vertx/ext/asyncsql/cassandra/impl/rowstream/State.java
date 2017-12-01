package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

/**
 * @author uday
 */
interface State<T extends StateWrapper> {

    void close(T stateWrapper);

    void execute(T stateWrapper);

    void pause(T stateWrapper);

    StateType type();
}
