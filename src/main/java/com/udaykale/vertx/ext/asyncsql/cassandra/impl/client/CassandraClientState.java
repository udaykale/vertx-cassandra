package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.Future;
import io.vertx.ext.sql.SQLConnection;

/**
 * @author uday
 */
interface CassandraClientState {

    void close(CassandraClientStateWrapper stateWrapper);

    Future<SQLConnection> createConnection(CassandraClientStateWrapper stateWrapper);

    StateType type();

    enum StateType {
        CREATING_CONNECTION, CLOSED
    }
}
