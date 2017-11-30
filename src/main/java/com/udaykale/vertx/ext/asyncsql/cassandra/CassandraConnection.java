package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.function.Function;

public interface CassandraConnection extends SQLConnection, Comparable<CassandraConnection> {

    SQLConnection queryStreamWithParams(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                        Handler<AsyncResult<SQLRowStream>> handler);

    SQLConnection queryWithParams(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                  Handler<AsyncResult<ResultSet>> resultHandler);

    SQLConnection batchWithParams(List<String> sqlStatements, List<JsonArray> args,
                                  Handler<AsyncResult<List<Integer>>> handler);

    int connectionId();

    @Override
    default int compareTo(CassandraConnection cassandraConnection) {
        return Integer.compare(cassandraConnection.connectionId(), connectionId());
    }
}
