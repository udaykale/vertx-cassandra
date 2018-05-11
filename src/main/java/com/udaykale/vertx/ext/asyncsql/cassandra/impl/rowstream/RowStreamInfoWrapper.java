package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
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
final class RowStreamInfoWrapper {
    private final Context context;
    private final ResultSet resultSet;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams;
    private final Function<Row, JsonArray> rowMapper;

    private RowStreamState state;
    private Handler<Void> endHandler;
    private Handler<JsonArray> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> resultSetClosedHandler;

    private RowStreamInfoWrapper(ResultSet resultSet, WorkerExecutor workerExecutor,
                                 Set<SQLRowStream> allRowStreams, RowStreamState state,
                                 Function<Row, JsonArray> rowMapper, Context context) {
        this.state = state;
        this.context = context;
        this.resultSet = resultSet;
        this.rowMapper = rowMapper;
        this.allRowStreams = allRowStreams;
        this.workerExecutor = workerExecutor;
    }

    static RowStreamInfoWrapper of(ResultSet resultSet, WorkerExecutor workerExecutor,
                                   Set<SQLRowStream> allRowStreams, RowStreamState state,
                                   Function<Row, JsonArray> rowMapper, Context context) {
        Objects.requireNonNull(state);
        Objects.requireNonNull(resultSet);
        Objects.requireNonNull(rowMapper);
        Objects.requireNonNull(allRowStreams);
        Objects.requireNonNull(workerExecutor);
        return new RowStreamInfoWrapper(resultSet, workerExecutor, allRowStreams, state, rowMapper, context);
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

    void close(CassandraRowStream cassandraRowStream, Handler<AsyncResult<Void>> closeHandler) {
        state.close(this, cassandraRowStream, closeHandler);
    }

    void execute() {
        state.execute(this);
    }

    void pause() {
        state.pause(this);
    }

    Function<Row, JsonArray> getRowMapper() {
        return rowMapper;
    }

    Context getContext() {
        return context;
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
        this.handler = handler;
    }

    void setExceptionHandler(Handler<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    void setResultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        this.resultSetClosedHandler = resultSetClosedHandler;
    }

    void setEndHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
    }

    Class<? extends RowStreamState> stateClass() {
        return state.getClass();
    }
}
