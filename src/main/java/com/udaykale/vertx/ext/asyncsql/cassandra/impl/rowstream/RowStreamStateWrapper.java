package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * @author uday
 */
final class RowStreamStateWrapper implements StateWrapper<RowStreamStateWrapper> {

    private State<RowStreamStateWrapper> state;
    private ResultSet resultSet;
    private WorkerExecutor workerExecutor;
    private Function<Row, JsonArray> rowMapper;
    private Handler<JsonArray> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> resultSetClosedHandler;
    private Handler<Void> endHandler;
    private Handler<AsyncResult<Void>> closeHandler;
    private Set<SQLRowStream> allRowStreams;

    private RowStreamStateWrapper(State<RowStreamStateWrapper> state) {
        this.state = Objects.requireNonNull(state);
    }

    static Builder builder(State<RowStreamStateWrapper> state) {
        return new Builder(state);
    }

    public State<RowStreamStateWrapper> getState() {
        return state;
    }

    public void setState(State<RowStreamStateWrapper> state) {
        this.state = Objects.requireNonNull(state);
    }

    ResultSet getResultSet() {
        return resultSet;
    }

    Set<SQLRowStream> getAllRowStreams() {
        return allRowStreams;
    }

    WorkerExecutor getWorkerExecutor() {
        return workerExecutor;
    }

    Function<Row, JsonArray> getRowMapper() {
        return rowMapper;
    }

    Handler<JsonArray> getHandler() {
        return handler;
    }

    Handler<Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    Handler<Void> getResultSetClosedHandler() {
        return resultSetClosedHandler;
    }

    Handler<Void> getEndHandler() {
        return endHandler;
    }

    Handler<AsyncResult<Void>> getCloseHandler() {
        return closeHandler;
    }

    static final class Builder {

        private State<RowStreamStateWrapper> state;

        private ResultSet resultSet;
        private WorkerExecutor workerExecutor;
        private Function<Row, JsonArray> rowMapper;
        private Handler<JsonArray> handler;
        private Handler<Throwable> exceptionHandler;
        private Handler<Void> resultSetClosedHandler;
        private Handler<Void> endHandler;
        private Handler<AsyncResult<Void>> closeHandler;
        private Set<SQLRowStream> allRowStreams;
        private Builder(State<RowStreamStateWrapper> state) {
            this.state = Objects.requireNonNull(state);
        }

        Builder withResultSet(ResultSet resultSet) {
            this.resultSet = Objects.requireNonNull(resultSet);
            return this;
        }

        Builder withWorkerExecutor(WorkerExecutor workerExecutor) {
            this.workerExecutor = Objects.requireNonNull(workerExecutor);
            return this;
        }

        Builder withRowMapper(Function<Row, JsonArray> rowMapper) {
            this.rowMapper = Objects.requireNonNull(rowMapper);
            return this;
        }

        Builder withHandler(Handler<JsonArray> handler) {
            this.handler = Objects.requireNonNull(handler);
            return this;
        }

        Builder withExceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
            return this;
        }

        Builder withResultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
            this.resultSetClosedHandler = Objects.requireNonNull(resultSetClosedHandler);
            return this;
        }

        Builder withEndHandler(Handler<Void> endHandler) {
            this.endHandler = Objects.requireNonNull(endHandler);
            return this;
        }

        Builder withCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
            this.closeHandler = Objects.requireNonNull(closeHandler);
            return this;
        }

        Builder withAllRowStreams(Set<SQLRowStream> allRowStreams) {
            this.allRowStreams = Objects.requireNonNull(allRowStreams);
            return this;
        }

        RowStreamStateWrapper build() {
            RowStreamStateWrapper rowStreamStateWrapper = new RowStreamStateWrapper(state);
            rowStreamStateWrapper.setResultSet(resultSet);
            rowStreamStateWrapper.setWorkerExecutor(workerExecutor);
            rowStreamStateWrapper.setRowMapper(rowMapper);
            rowStreamStateWrapper.setHandler(handler);
            rowStreamStateWrapper.setExceptionHandler(exceptionHandler);
            rowStreamStateWrapper.setResultSetClosedHandler(resultSetClosedHandler);
            rowStreamStateWrapper.setEndHandler(endHandler);
            rowStreamStateWrapper.setCloseHandler(closeHandler);
            rowStreamStateWrapper.setAllRowStreams(allRowStreams);
            return rowStreamStateWrapper;
        }

    }
    private void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    private void setWorkerExecutor(WorkerExecutor workerExecutor) {
        this.workerExecutor = workerExecutor;
    }

    private void setRowMapper(Function<Row, JsonArray> rowMapper) {
        this.rowMapper = rowMapper;
    }

    private void setHandler(Handler<JsonArray> handler) {
        this.handler = handler;
    }

    private void setExceptionHandler(Handler<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    private void setResultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        this.resultSetClosedHandler = resultSetClosedHandler;
    }

    private void setAllRowStreams(Set<SQLRowStream> allRowStreams) {
        this.allRowStreams = allRowStreams;
    }

    private void setEndHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
    }

    private void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = closeHandler;
    }
}
