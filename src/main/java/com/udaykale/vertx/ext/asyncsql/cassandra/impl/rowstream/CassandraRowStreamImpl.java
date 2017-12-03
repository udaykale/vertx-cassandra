package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamHelper.stateWrapper;

/**
 * @author uday
 */
public final class CassandraRowStreamImpl implements CassandraRowStream {

    private final ResultSet resultSet;
    private final List<String> columns;
    private final int rowStreamId;

    private RowStreamStateWrapper stateWrapper;

    public CassandraRowStreamImpl(int rowStreamId, ResultSet resultSet, WorkerExecutor workerExecutor,
                                  Set<SQLRowStream> allRowStreams, Function<Row, JsonArray> rowMapper) {
        this.resultSet = resultSet;
        this.rowStreamId = rowStreamId;
        this.columns = RowStreamHelper.columns(resultSet);
        this.stateWrapper = stateWrapper(workerExecutor, allRowStreams, rowMapper, this, columns, resultSet);
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
            RowStreamState currentState = stateWrapper.getState();

            if (currentState.type() == RowStreamState.StateType.EXECUTING) {
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
            RowStreamState currentState = stateWrapper.getState();

            if (currentState.type() == RowStreamState.StateType.PAUSED) {
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
            RowStreamState currentState = stateWrapper.getState();

            if (currentState.type() == RowStreamState.StateType.PAUSED) {
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
            RowStreamState currentState = stateWrapper.getState();
            currentState.close(stateWrapper);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        stateWrapper.setCloseHandler(closeHandler);
        close();
    }

    @Override
    public int getStreamId() {
        return rowStreamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CassandraRowStreamImpl that = (CassandraRowStreamImpl) o;

        return rowStreamId == that.rowStreamId;
    }

    @Override
    public int hashCode() {
        return rowStreamId;
    }
}
