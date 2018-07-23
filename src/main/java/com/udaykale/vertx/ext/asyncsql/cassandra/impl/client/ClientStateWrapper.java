package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLConnection;

import java.util.Objects;

final class ClientStateWrapper {

    private CassandraClientState state;

    private ClientStateWrapper(CassandraClientState state) {
        this.state = Objects.requireNonNull(state);
    }

    static ClientStateWrapper of(CassandraClientState state) {
        return new ClientStateWrapper(state);
    }

    void close(Handler<AsyncResult<Void>> closeHandler) {
        state.close(this, closeHandler);
    }

    void createConnection(Handler<AsyncResult<SQLConnection>> handler) {
        state.createConnection(this, handler);
    }

    void setState(CassandraClientState clientState) {
        this.state = Objects.requireNonNull(clientState);
    }
}
