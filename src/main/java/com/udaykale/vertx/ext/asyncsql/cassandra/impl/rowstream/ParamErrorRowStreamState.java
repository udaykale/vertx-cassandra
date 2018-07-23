package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

public class ParamErrorRowStreamState implements RowStreamState {

    private Throwable error;

    private ParamErrorRowStreamState(Throwable error) {
        this.error = error;
    }

    static ParamErrorRowStreamState instance(Throwable error) {
        return new ParamErrorRowStreamState(error);
    }

    @Override
    public void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler, Handler<Throwable> exceptionHandler) {
        // TODO: Call handler with failure instead of throwing an exception
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void execute(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler,
                        Handler<Void> endHandler, Handler<JsonArray> handler, Handler<Void> resultSetClosedHandler,
                        Handler<AsyncResult<Void>> closeHandler) {
        // TODO: Call handler with failure instead of throwing an exception
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void pause(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler) {
        // TODO: Call handler with failure instead of throwing an exception
        // do nothing since an error was already thrown because parameters were set wrong
    }
}
