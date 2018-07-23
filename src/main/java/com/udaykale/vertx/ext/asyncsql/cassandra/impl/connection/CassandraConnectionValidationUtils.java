package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class CassandraConnectionValidationUtils {

    private CassandraConnectionValidationUtils() {
    }

    static <T> void validateQueryAndRun(String query, Context context, Handler<AsyncResult<T>> handler,
                                        Handler<AsyncResult<T>> nextOperation) {
        Objects.requireNonNull(handler);
        try {
            Objects.requireNonNull(query);
            assert !query.isEmpty();
            context.runOnContext(future -> nextOperation.handle(Future.succeededFuture()));
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static <T> void validateQueryParamsAndRun(String query, JsonArray params, Context context, Handler<AsyncResult<T>> handler,
                                              Handler<AsyncResult<T>> nextOperation) {
        try {
            Objects.requireNonNull(params);
            validateQueryAndRun(query, context, handler, nextOperation);
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static <T> void validateQueryParamsRowMapper(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                                 Context context, Handler<AsyncResult<T>> handler,
                                                 Handler<AsyncResult<T>> nextOperation) {
        try {
            Objects.requireNonNull(rowMapper);
            validateQueryParamsAndRun(query, params, context, handler, nextOperation);
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static void validateBatch(List<String> sqlStatements, Context context, Handler<AsyncResult<List<Integer>>> handler,
                              Handler<AsyncResult<List<Integer>>> nextOperation) {
        Objects.requireNonNull(handler);
        try {
            Objects.requireNonNull(sqlStatements);
            assert !sqlStatements.isEmpty();
            assert sqlStatements.stream().allMatch(String::isEmpty);
            context.runOnContext(future -> nextOperation.handle(Future.succeededFuture()));
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }
}
