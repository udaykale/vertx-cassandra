package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.Future;
import io.vertx.ext.sql.SQLConnection;

/**
 * @author uday
 */
interface CassandraClientState {

    void close(ClientInfo clientInfo);

    Future<SQLConnection> createConnection(ClientInfo clientInfo);

    StateType type();

    enum StateType {
        CREATING_CONNECTION, CLOSED
    }
}
