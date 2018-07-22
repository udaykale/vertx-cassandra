package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.function.Function;

/**
 * @author uday
 */
interface CassandraConnectionState {

    void close(ConnectionStateWrapper connectionStateWrapper, CassandraConnection connection, Handler<AsyncResult<Void>> closeHandler);

    void stream(ConnectionStateWrapper connectionStateWrapper, List<String> queries, List<JsonArray> params,
                SQLOptions sqlOptions, Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler);
}
