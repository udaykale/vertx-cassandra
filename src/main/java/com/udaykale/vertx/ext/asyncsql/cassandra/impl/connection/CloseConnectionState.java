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

class CloseConnectionState implements CassandraConnectionState {

    private CloseConnectionState() {
    }

    static CloseConnectionState instance() {
        return new CloseConnectionState();
    }

    @Override
    public void close(ConnectionStateWrapper connectionStateWrapper, CassandraConnection connection,
                      Handler<AsyncResult<Void>> closeHandler) {
        throw new IllegalStateException("Cannot re-close connection when it is already closed");
    }

    @Override
    public void stream(ConnectionStateWrapper connectionStateWrapper, List<String> queries, List<JsonArray> params,
                       SQLOptions sqlOptions, Function<Row, JsonArray> rowMapper,
                       Handler<AsyncResult<SQLRowStream>> handler) {
        RuntimeException e = new IllegalStateException("Cannot stream from connection when it is closed");

        if (handler != null) {
            handler.handle(Future.failedFuture(e));
        } else {
            throw e;
        }
    }
}
