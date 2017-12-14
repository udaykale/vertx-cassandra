package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author uday
 */
final class CassandraConnectionStreamHelper {

    private final AtomicBoolean lock;

    private CassandraConnectionStreamHelper(AtomicBoolean lock) {
        this.lock = Objects.requireNonNull(lock);
    }

    static CassandraConnectionStreamHelper of(AtomicBoolean lock) {
        Objects.requireNonNull(lock);
        return new CassandraConnectionStreamHelper(lock);
    }

    void queryStreamWithParams(ConnectionInfo connectionInfo, List<String> queries, List<JsonArray> params,
                               Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler) {
        synchronized (lock) {
            Context context = connectionInfo.getContext();
            context.runOnContext(v -> connectionInfo.getState()
                    .stream(connectionInfo, queries, params, rowMapper, handler));
        }
    }
}
