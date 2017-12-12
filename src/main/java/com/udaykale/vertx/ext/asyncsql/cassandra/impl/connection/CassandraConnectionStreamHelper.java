package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author uday
 */
final class CassandraConnectionStreamHelper {

    private final Integer connectionId;

    private CassandraConnectionStreamHelper(Integer connectionId) {
        this.connectionId = Objects.requireNonNull(connectionId);
    }

    static CassandraConnectionStreamHelper of(Integer connectionId) {
        Objects.requireNonNull(connectionId);
        return new CassandraConnectionStreamHelper(connectionId);
    }

    void queryStreamWithParams(ConnectionInfo connectionInfo, List<String> queries, List<JsonArray> params,
                               Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler) {
        synchronized (connectionId) {
            connectionInfo.getState().stream(connectionInfo, queries, params, rowMapper, handler);
        }
    }
}
