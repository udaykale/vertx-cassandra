package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author uday
 */
final class ConnectionStateWrapper {

    private CassandraConnectionState state;

    private ConnectionStateWrapper(CassandraConnectionState currentState) {
        this.state = currentState;
    }

    static ConnectionStateWrapper of(CassandraConnectionState currentState) {
        return new ConnectionStateWrapper(currentState);
    }

    void close(CassandraConnection connection, Handler<AsyncResult<Void>> closeHandler) {
        state.close(this, connection, closeHandler);
    }

    void stream(List<String> queries, List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                SQLOptions sqlOptions, Handler<AsyncResult<SQLRowStream>> handler) {
        state.stream(this, queries, params, sqlOptions, rowMapper, handler);
    }

    void setState(CassandraConnectionState state) {
        Objects.requireNonNull(state);
        this.state = state;
    }
}
