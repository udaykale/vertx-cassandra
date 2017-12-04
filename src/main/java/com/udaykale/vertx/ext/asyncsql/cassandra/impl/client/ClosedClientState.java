package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.Future;
import io.vertx.ext.sql.SQLConnection;

/**
 * @author uday
 */
final class ClosedClientState implements CassandraClientState {

    private ClosedClientState() {
    }

    static CassandraClientState instance() {
        return new ClosedClientState();
    }

    @Override
    public void close(ClientInfo clientInfo) {
        throw new IllegalStateException("Cannot re-close client when it is already closed");
    }

    @Override
    public Future<SQLConnection> createConnection(ClientInfo clientInfo) {
        throw new IllegalStateException("Cannot create connection when client is already closed");
    }

    @Override
    public StateType type() {
        return StateType.CLOSED;
    }
}
