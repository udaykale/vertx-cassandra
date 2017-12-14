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

/**
 * @author uday
 */
final class CassandraConnectionValidationUtils {

    private CassandraConnectionValidationUtils() {
    }

    static <T> void validateQuery(String query, Handler<AsyncResult<T>> handler) {
        Objects.requireNonNull(handler);
        try {
            Objects.requireNonNull(query);
            assert !query.isEmpty();
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static <T> void validateQueryParams(String query, JsonArray params,
                                        Handler<AsyncResult<T>> handler) {
        validateQuery(query, handler);
        try {
            Objects.requireNonNull(params);
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static <T> void validateQueryParamsRowMapper(String query, JsonArray params,
                                                 Function<Row, JsonArray> rowMapper,
                                                 Handler<AsyncResult<T>> handler) {
        validateQueryParams(query, params, handler);
        try {
            Objects.requireNonNull(rowMapper);
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }

    static void validateBatch(List<String> sqlStatements,
                              Handler<AsyncResult<List<Integer>>> handler) {
        Objects.requireNonNull(handler);
        try {
            Objects.requireNonNull(sqlStatements);
            assert !sqlStatements.isEmpty();
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        }
    }
}
