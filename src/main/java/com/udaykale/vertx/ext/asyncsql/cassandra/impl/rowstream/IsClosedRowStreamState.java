package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

final class IsClosedRowStreamState implements RowStreamState {

    private IsClosedRowStreamState() {
    }

    static IsClosedRowStreamState instance() {
        return new IsClosedRowStreamState();
    }

    @Override
    public void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler, Handler<Throwable> exceptionHandler) {
        handleIllegalStateException(rowStreamStateWrapper, "Cannot re-close when stream is already closed", exceptionHandler);
    }

    @Override
    public void execute(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler,
                        Handler<Void> endHandler, Handler<JsonArray> handler, Handler<Void> resultSetClosedHandler,
                        Handler<AsyncResult<Void>> closeHandler) {
        handleIllegalStateException(rowStreamStateWrapper, "Cannot execute when stream is already closed", exceptionHandler);
    }

    @Override
    public void pause(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler) {
        handleIllegalStateException(rowStreamStateWrapper, "Cannot pause when stream is already closed", exceptionHandler);
    }
}
