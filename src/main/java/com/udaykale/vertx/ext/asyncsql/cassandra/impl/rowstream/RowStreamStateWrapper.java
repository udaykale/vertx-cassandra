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
final class RowStreamStateWrapper {

    private RowStreamState state;
    private ResultSet resultSet;
    private WorkerExecutor workerExecutor;
    private Function<Row, JsonArray> rowMapper;
    private Handler<JsonArray> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> resultSetClosedHandler;
    private Handler<Void> endHandler;
    private Handler<AsyncResult<Void>> closeHandler;
    private Set<SQLRowStream> allRowStreams;

    RowStreamStateWrapper(RowStreamState state) {
        this.state = Objects.requireNonNull(state);
    }

    public RowStreamState getState() {
        return state;
    }

    public void setState(RowStreamState state) {
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

    void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    void setWorkerExecutor(WorkerExecutor workerExecutor) {
        this.workerExecutor = workerExecutor;
    }

    void setRowMapper(Function<Row, JsonArray> rowMapper) {
        this.rowMapper = rowMapper;
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

    void setAllRowStreams(Set<SQLRowStream> allRowStreams) {
        this.allRowStreams = allRowStreams;
    }

    void setEndHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
    }

    void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = closeHandler;
    }
}
