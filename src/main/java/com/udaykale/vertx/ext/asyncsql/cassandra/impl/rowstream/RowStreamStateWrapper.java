package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.Objects;

final class RowStreamStateWrapper {

    private RowStreamState state;

    private RowStreamStateWrapper(RowStreamState state) {
        this.state = state;
    }

    static RowStreamStateWrapper of(RowStreamState state) {
        return new RowStreamStateWrapper(state);
    }

    void close(CassandraRowStream cassandraRowStream, Handler<AsyncResult<Void>> closeHandler,
               Handler<Throwable> exceptionHandler) {
        state.close(this, cassandraRowStream, closeHandler, exceptionHandler);
    }

    void execute(Handler<Throwable> exceptionHandler, Handler<Void> endHandler, Handler<JsonArray> handler,
                 Handler<Void> resultSetClosedHandler, Handler<AsyncResult<Void>> closeHandler) {
        state.execute(this, exceptionHandler, endHandler, handler, resultSetClosedHandler, closeHandler);
    }

    void pause(Handler<Throwable> exceptionHandler) {
        state.pause(this, exceptionHandler);
    }

    void setState(RowStreamState state) {
        this.state = Objects.requireNonNull(state);
    }

    Class<? extends RowStreamState> stateClass() {
        return state.getClass();
    }
}
