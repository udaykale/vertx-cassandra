package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

public interface RowStreamState {

    void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream cassandraRowStream,
               Handler<AsyncResult<Void>> closeHandler, Handler<Throwable> exceptionHandler);

    void execute(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler,
                 Handler<Void> endHandler, Handler<JsonArray> handler, Handler<Void> resultSetClosedHandler,
                 Handler<AsyncResult<Void>> closeHandler);

    void pause(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler);
}
