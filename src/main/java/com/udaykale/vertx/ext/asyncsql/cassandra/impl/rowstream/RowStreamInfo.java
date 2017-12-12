package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author uday
 */
final class RowStreamInfo {
    private final ResultSet resultSet;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams;
    private final Function<Row, JsonArray> rowMapper;

    private RowStreamState state;
    private Handler<Void> endHandler;
    private Handler<JsonArray> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> resultSetClosedHandler;

    private RowStreamInfo(ResultSet resultSet, WorkerExecutor workerExecutor, Set<SQLRowStream> allRowStreams,
                          RowStreamState state, Function<Row, JsonArray> rowMapper) {
        this.state = state;
        this.resultSet = resultSet;
        this.rowMapper = rowMapper;
        this.allRowStreams = allRowStreams;
        this.workerExecutor = workerExecutor;
    }

    static RowStreamInfo of(WorkerExecutor workerExecutor, Set<SQLRowStream> allRowStreams, ResultSet resultSet,
                            RowStreamState state, Function<Row, JsonArray> rowMapper) {
        Objects.requireNonNull(resultSet);
        Objects.requireNonNull(rowMapper);
        Objects.requireNonNull(state);
        Objects.requireNonNull(allRowStreams);
        Objects.requireNonNull(workerExecutor);
        return new RowStreamInfo(resultSet, workerExecutor, allRowStreams, state, rowMapper);
    }

    WorkerExecutor getWorkerExecutor() {
        return workerExecutor;
    }

    Set<SQLRowStream> getAllRowStreams() {
        return allRowStreams;
    }

    ResultSet getResultSet() {
        return resultSet;
    }

    RowStreamState getState() {
        return state;
    }

    Function<Row, JsonArray> getRowMapper() {
        return rowMapper;
    }

    Optional<Handler<JsonArray>> getHandler() {
        return Optional.ofNullable(handler);
    }

    Optional<Handler<Throwable>> getExceptionHandler() {
        return Optional.ofNullable(exceptionHandler);
    }

    Optional<Handler<Void>> getResultSetClosedHandler() {
        return Optional.ofNullable(resultSetClosedHandler);
    }

    Optional<Handler<Void>> getEndHandler() {
        return Optional.ofNullable(endHandler);
    }

    void setState(RowStreamState state) {
        Objects.requireNonNull(state);
        this.state = state;
    }

    void setHandler(Handler<JsonArray> handler) {
        Objects.requireNonNull(handler);
        this.handler = handler;
    }

    void setExceptionHandler(Handler<Throwable> exceptionHandler) {
        Objects.requireNonNull(exceptionHandler);
        this.exceptionHandler = exceptionHandler;
    }

    void setResultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        Objects.requireNonNull(resultSetClosedHandler);
        this.resultSetClosedHandler = resultSetClosedHandler;
    }

    void setEndHandler(Handler<Void> endHandler) {
        Objects.requireNonNull(endHandler);
        this.endHandler = endHandler;
    }
}
