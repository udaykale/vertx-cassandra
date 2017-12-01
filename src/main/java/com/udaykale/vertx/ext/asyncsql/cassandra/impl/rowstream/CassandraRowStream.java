package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author uday
 */
public final class CassandraRowStream implements SQLRowStream, Comparable<CassandraRowStream> {

    private final ResultSet resultSet;
    private final List<String> columns;
    private final int rowStreamId;

    private RowStreamStateWrapper stateWrapper;

    public CassandraRowStream(int rowStreamId, ResultSet resultSet, WorkerExecutor workerExecutor,
                              Set<SQLRowStream> allRowStreams, Function<Row, JsonArray> rowMapper) {
        this.resultSet = resultSet;
        this.rowStreamId = rowStreamId;
        this.columns = RowStreamHelper.columns(resultSet);
        this.stateWrapper = RowStreamHelper.stateWrapper(workerExecutor, allRowStreams, rowMapper, this, columns, resultSet);
    }

    @Override
    public SQLRowStream exceptionHandler(Handler<Throwable> exceptionHandler) {
        stateWrapper.setExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public SQLRowStream handler(Handler<JsonArray> handler) {
        stateWrapper.setHandler(handler);
        return resume();
    }

    @Override
    public SQLRowStream pause() {
        synchronized (this) {
            State<RowStreamStateWrapper> currentState = stateWrapper.getState();

            if (currentState.type() == StateType.IS_EXECUTING) {
                currentState.pause(stateWrapper);
            } else {
                // no need to pause since its already paused or closed
            }
        }

        return this;
    }

    @Override
    public SQLRowStream resume() {
        synchronized (this) {
            State<RowStreamStateWrapper> currentState = stateWrapper.getState();

            if (currentState.type() == StateType.IS_PAUSED) {
                currentState.execute(stateWrapper);
            } else {
                // no need to resume execution since already executing or closed
            }
        }

        return this;
    }

    @Override
    public SQLRowStream endHandler(Handler<Void> endHandler) {
        stateWrapper.setEndHandler(endHandler);
        return this;
    }

    @Override
    public int column(String name) {
        return resultSet.getColumnDefinitions().getIndexOf(name) - 1;
    }

    @Override
    public List<String> columns() {
        return columns;
    }

    @Override
    public SQLRowStream resultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        stateWrapper.setResultSetClosedHandler(resultSetClosedHandler);
        return this;
    }

    @Override
    public void moreResults() {
        boolean wasLastPage = resultSet.getExecutionInfo().getPagingState() == null;

        if (!wasLastPage) {
            resultSet.fetchMoreResults();
        }

        synchronized (this) {
            State<RowStreamStateWrapper> currentState = stateWrapper.getState();

            if (currentState.type() == StateType.IS_PAUSED) {
                // restart execution since its currently paused
                currentState.execute(stateWrapper);
            } else {
                // no need to resume execution since already executing or closed
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            State<RowStreamStateWrapper> currentState = stateWrapper.getState();
            currentState.close(stateWrapper);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        stateWrapper.setCloseHandler(closeHandler);
        close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CassandraRowStream that = (CassandraRowStream) o;

        return rowStreamId == that.rowStreamId;
    }

    @Override
    public int hashCode() {
        return rowStreamId;
    }

    @Override
    public int compareTo(CassandraRowStream that) {
        return this.rowStreamId - that.rowStreamId;
    }
}
