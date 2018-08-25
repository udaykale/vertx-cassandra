package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLOptions;
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

final class CassandraConnectionUtil {

    private CassandraConnectionUtil() {
    }

    static void handleUpdate(Handler<AsyncResult<UpdateResult>> handler, AsyncResult<ResultSet> future) {
        if (future.succeeded()) {
            UpdateResult updateResult = new UpdateResult();
            updateResult.setUpdated(-1);
            handler.handle(Future.succeededFuture(updateResult));
        } else {
            handler.handle(Future.failedFuture(future.cause()));
        }
    }

    static void handleBatch(int resultSize, Handler<AsyncResult<List<Integer>>> handler,
                            AsyncResult<ResultSet> future) {
        Future<List<Integer>> result;

        if (future.succeeded()) {
            result = Future.succeededFuture(nCopies(resultSize, -1));
        } else {
            result = Future.failedFuture(future.cause());
        }

        result.setHandler(handler);
    }

    static void queryWithParams(AtomicBoolean lock, ConnectionStateWrapper connectionStateWrapper, List<String> query,
                                List<JsonArray> params, Function<Row, JsonArray> rowMapper, SQLOptions sqlOptions,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        Objects.requireNonNull(query);
        List<JsonArray> jsonArrays = new LinkedList<>();
        Future<ResultSet> result = Future.future();

        CassandraConnectionStreamHelper.of(lock).queryStreamWithParams(connectionStateWrapper, query, params,
                sqlOptions, rowMapper, queryResult -> {
                    if (queryResult.failed()) {
                        result.fail(queryResult.cause());
                    } else {
                        SQLRowStream sqlRowStream = queryResult.result();

                        sqlRowStream.resultSetClosedHandler(v -> sqlRowStream.moreResults())
                                .handler(jsonArrays::add)
                                .endHandler(e -> streamEndHandler(jsonArrays, result, sqlRowStream))
                                .exceptionHandler(result::fail);
                    }
                    result.setHandler(resultHandler);
                });
    }

    static void queryWithParams(AtomicBoolean lock, ConnectionStateWrapper connectionStateWrapper, String query,
                                JsonArray params, Function<Row, JsonArray> rowMapper, SQLOptions sqlOptions,
                                Handler<AsyncResult<ResultSet>> resultHandler) {
        queryWithParams(lock, connectionStateWrapper, singletonList(query), emptyListIfNull(params),
                rowMapper, sqlOptions, resultHandler);
    }

    private static <T> List<T> emptyListIfNull(T element) {
        return element == null ? EMPTY_LIST : singletonList(element);
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
