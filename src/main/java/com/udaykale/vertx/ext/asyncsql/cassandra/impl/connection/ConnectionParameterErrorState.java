package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.function.Function;

class ConnectionParameterErrorState implements CassandraConnectionState {

    private Throwable error;

    private ConnectionParameterErrorState(Throwable error) {
        this.error = error;
    }

    static ConnectionParameterErrorState instance(Throwable error) {
        return new ConnectionParameterErrorState(error);
    }

    @Override
    public void close(ConnectionStateWrapper connectionStateWrapper, CassandraConnection connection,
                      Handler<AsyncResult<Void>> closeHandler) {
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void stream(ConnectionStateWrapper connectionStateWrapper, List<String> queries, List<JsonArray> params,
                       SQLOptions sqlOptions, Function<Row, JsonArray> rowMapper,
                       Handler<AsyncResult<SQLRowStream>> handler) {
        handler.handle(Future.failedFuture(error));
    }
}
