package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author uday
 */
final class CassandraConnectionValidationUtils {

    static <T> void validateQuery(String query, Handler<AsyncResult<T>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(handler);
    }

    static <T> void validateQueryParams(String query, JsonArray params, Handler<AsyncResult<T>> handler) {
        validateQuery(query, handler);
        Objects.requireNonNull(params);
    }

    static <T> void validateQueryParamsRowMapper(String query, JsonArray params,
                                                 Function<Row, JsonArray> rowMapper,
                                                 Handler<AsyncResult<T>> handler) {
        validateQueryParams(query, params, handler);
        Objects.requireNonNull(rowMapper);
    }

    static void validateBatch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
        Objects.requireNonNull(sqlStatements);
        assert !sqlStatements.isEmpty();
        Objects.requireNonNull(handler);
    }
}
