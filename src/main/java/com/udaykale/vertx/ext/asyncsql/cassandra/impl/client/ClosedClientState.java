package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
    public void close(ClientInfo clientInfo, Handler<AsyncResult<Void>> closeHandler) {
        throw new IllegalStateException("Cannot re-close client when it is already closed");
    }

    @Override
    public void createConnection(ClientInfo clientInfo, Handler<AsyncResult<SQLConnection>> handler) {
        Context context = clientInfo.getContext();
        Exception e = new IllegalStateException("Cannot create connection when client is already closed");
        Future<SQLConnection> result = Future.failedFuture(e);
        context.runOnContext(action -> result.setHandler(handler));
    }
}
