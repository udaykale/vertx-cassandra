package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.CassandraRowStreamImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraStatementHelper.generateStatement;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

/**
 * @author uday
 */
final class CassandraConnectionHelper {

    static void handleUpdate(Handler<AsyncResult<UpdateResult>> resultHandler,
                             Context context, AsyncResult<ResultSet> future) {
        Future<UpdateResult> result;

        if (future.succeeded()) {
            UpdateResult updateResult = new UpdateResult();
            updateResult.setUpdated(-1);
            result = Future.succeededFuture(updateResult);
        } else {
            result = Future.failedFuture(future.cause());
        }

        context.runOnContext(v -> result.setHandler(resultHandler));
    }

    static void handleBatch(int resultSize, Handler<AsyncResult<List<Integer>>> handler,
                            Context context, AsyncResult<ResultSet> future) {
        Future<List<Integer>> result;

        if (future.succeeded()) {
            result = Future.succeededFuture(nCopies(resultSize, -1));
        } else {
            result = Future.failedFuture(future.cause());
        }

        context.runOnContext(v -> result.setHandler(handler));
    }

    static void handleQuery(Handler<AsyncResult<Void>> resultHandler,
                            AsyncResult<ResultSet> future, Context context) {
        Future<Void> result = Future.future();

        if (future.succeeded()) {
            result.complete();
        } else {
            result.fail(future.cause());
        }

        context.runOnContext(v -> result.setHandler(resultHandler));
    }

    static void queryWithParams(ConnectionInfo connectionInfo, List<String> query,
                                List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        Objects.requireNonNull(query);
        List<JsonArray> jsonArrays = new LinkedList<>();
        Future<ResultSet> result = Future.future();
        Context context = connectionInfo.getContext();

        queryStreamWithParams(connectionInfo, query, params, rowMapper, queryResult -> {
            if (queryResult.succeeded()) {
                SQLRowStream sqlRowStream = queryResult.result();

                sqlRowStream.resultSetClosedHandler(v -> sqlRowStream.moreResults())
                        .handler(jsonArrays::add)
                        .endHandler(e -> streamEndHandler(jsonArrays, result, sqlRowStream))
                        .exceptionHandler(result::fail);
            } else {
                result.fail(queryResult.cause());
            }
        });

        context.runOnContext(v -> result.setHandler(resultHandler));
    }

    static void queryWithParams(ConnectionInfo connectionInfo, String query,
                                JsonArray params, Function<Row, JsonArray> rowMapper,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        queryWithParams(connectionInfo, singletonList(query),
                params == null ? EMPTY_LIST : singletonList(params), rowMapper, resultHandler);
    }

    static <T> List<T> emptyListIfNull(T element) {
        return element == null ? EMPTY_LIST : singletonList(element);
    }

    static void queryStreamWithParams(ConnectionInfo connectionInfo,
                                      List<String> queries, List<JsonArray> params,
                                      Function<Row, JsonArray> rowMapper,
                                      Handler<AsyncResult<SQLRowStream>> handler) {

        CassandraConnection cassandraConnection = connectionInfo.getConnection();
        CassandraConnectionStreamHelper cassandraConnectionStreamHelper =
                new CassandraConnectionStreamHelper(cassandraConnection);

        cassandraConnectionStreamHelper.queryStreamWithParams(connectionInfo, queries, params, rowMapper, handler);
    }

    private static void streamEndHandler(List<JsonArray> jsonArrays, Future<ResultSet> result,
                                         SQLRowStream sqlRowStream) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnNames(sqlRowStream.columns());
        resultSet.setResults(jsonArrays);
        result.complete(resultSet);
        sqlRowStream.close();
    }
}
