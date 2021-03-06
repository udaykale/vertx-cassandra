package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLConnection;

final class ClosedClientState implements CassandraClientState {

    private ClosedClientState() {
    }

    static CassandraClientState instance() {
        return new ClosedClientState();
    }

    /**
     * Calls the close handler with a illegal state exception
     * This call is illegal because closing of a client after a client is already closed is not possible
     *
     * @param clientStateWrapper Client info wrapper
     * @param closeHandler       Client close handler
     */
    @Override
    public void close(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<Void>> closeHandler) {
        RuntimeException e = new IllegalStateException("Cannot re-close client when it is already closed");

        if (closeHandler != null) {
            closeHandler.handle(Future.failedFuture(e));
        } else {
            throw e;
        }
    }

    /**
     * Calls the handler with a illegal state exception
     * This call is illegal because creation of a connection after a client is closed is not possible
     * All input arguments are validated by the calling code of this method
     *
     * @param clientStateWrapper Client info wrapper
     * @param handler            Handler after connection creation
     */
    @Override
    public void createConnection(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<SQLConnection>> handler) {
        Exception e = new IllegalStateException("Cannot create connection when client is already closed");
        Future<SQLConnection> result = Future.failedFuture(e);
        result.setHandler(handler);
    }
}
