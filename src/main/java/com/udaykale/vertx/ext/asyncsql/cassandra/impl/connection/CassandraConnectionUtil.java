package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

/**
 * @author uday
 */
final class CassandraConnectionUtil {

    private CassandraConnectionUtil() {
    }

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
                            Context context, AsyncResult<ResultSet> future) {
        Future<Void> result = Future.future();

        if (future.succeeded()) {
            result.complete();
        } else {
            result.fail(future.cause());
        }

        context.runOnContext(v -> result.setHandler(resultHandler));
    }

    static void queryWithParams(ConnectionInfoWrapper connectionInfoWrapper, List<String> query,
                                List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        Objects.requireNonNull(query);
        List<JsonArray> jsonArrays = new LinkedList<>();
        Future<ResultSet> result = Future.future();
        Context context = connectionInfoWrapper.getContext();

        queryStreamWithParams(connectionInfoWrapper, query, params, rowMapper, queryResult -> {
            if (queryResult.failed()) {
                result.fail(queryResult.cause());
            } else {
                SQLRowStream sqlRowStream = queryResult.result();

                sqlRowStream.resultSetClosedHandler(v -> sqlRowStream.moreResults())
                        .handler(jsonArrays::add)
                        .endHandler(e -> streamEndHandler(jsonArrays, result, sqlRowStream))
                        .exceptionHandler(result::fail);
            }
            context.runOnContext(v -> result.setHandler(resultHandler));
        });
    }

    static void queryWithParams(ConnectionInfoWrapper connectionInfoWrapper, String query,
                                JsonArray params, Function<Row, JsonArray> rowMapper,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        queryWithParams(connectionInfoWrapper, singletonList(query), emptyListIfNull(params), rowMapper, resultHandler);
    }

    private static <T> List<T> emptyListIfNull(T element) {
        return element == null ? EMPTY_LIST : singletonList(element);
    }

    static void queryStreamWithParams(ConnectionInfoWrapper connectionInfoWrapper,
                                      List<String> queries, List<JsonArray> params,
                                      Function<Row, JsonArray> rowMapper,
                                      Handler<AsyncResult<SQLRowStream>> handler) {
        AtomicBoolean lock = connectionInfoWrapper.getLock();
        CassandraConnectionStreamHelper helper = CassandraConnectionStreamHelper.of(lock);
        helper.queryStreamWithParams(connectionInfoWrapper, queries, params, rowMapper, handler);
    }

    private static void streamEndHandler(List<JsonArray> jsonArrays, Future<ResultSet> result,
                                         SQLRowStream sqlRowStream) {
        ResultSet resultSet = new ResultSet()
                .setColumnNames(sqlRowStream.columns())
                .setResults(jsonArrays);
        result.complete(resultSet);
        sqlRowStream.close();
    }
}
