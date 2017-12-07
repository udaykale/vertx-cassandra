package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamHelper.defaultRowMapper;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamHelper.rowStreamInfo;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamHelper.rowStreamMapper;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * @author uday
 */
public final class CassandraRowStreamImpl implements CassandraRowStream {

    private final ResultSet resultSet;
    private final int rowStreamId;

    private final RowStreamInfo rowStreamInfo;

    private List<String> columnNames;

    public CassandraRowStreamImpl(int rowStreamId, ResultSet resultSet, WorkerExecutor workerExecutor,
                                  Set<SQLRowStream> allRowStreams, Function<Row, JsonArray> rowMapper) {
        this.resultSet = resultSet;
        this.rowStreamId = rowStreamId;
        this.rowStreamInfo = rowStreamInfo(workerExecutor, allRowStreams, rowMapper, this, resultSet);
    }

    @Override
    public SQLRowStream exceptionHandler(Handler<Throwable> exceptionHandler) {
        rowStreamInfo.setExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public SQLRowStream handler(Handler<JsonArray> handler) {
        rowStreamInfo.setHandler(handler);
        return resume();
    }

    @Override
    public SQLRowStream pause() {
        synchronized (this) {
            RowStreamState currentState = rowStreamInfo.getState();

            if (currentState.type() == RowStreamState.StateType.EXECUTING) {
                currentState.pause(rowStreamInfo);
            }  // no need to pause since its already paused or closed so no else part
        }

        return this;
    }

    @Override
    public SQLRowStream resume() {
        synchronized (this) {
            RowStreamState currentState = rowStreamInfo.getState();
            if (currentState.type() == RowStreamState.StateType.PAUSED) {
                currentState.execute(rowStreamInfo);
            }  // no need to resume execution since already executing or closed so no else part
        }

        return this;
    }

    @Override
    public SQLRowStream endHandler(Handler<Void> endHandler) {
        rowStreamInfo.setEndHandler(endHandler);
        return this;
    }

    @Override
    public int column(String name) {
        return resultSet.getColumnDefinitions().getIndexOf(name) - 1;
    }

    @Override
    public List<String> columns() {
        if (columnNames == null) {
            synchronized (this) {
                columnNames = resultSet.getColumnDefinitions()
                        .asList().stream()
                        .map(ColumnDefinitions.Definition::getName)
                        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
            }
        } // no need for else

        return columnNames;
    }

    @Override
    public SQLRowStream resultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        rowStreamInfo.setResultSetClosedHandler(resultSetClosedHandler);
        return this;
    }

    @Override
    public void moreResults() {
        boolean wasLastPage = resultSet.getExecutionInfo().getPagingState() == null;

        if (!wasLastPage) {
            resultSet.fetchMoreResults();
        }

        resume();
    }

    @Override
    public void close() {
        synchronized (this) {
            RowStreamState currentState = rowStreamInfo.getState();
            currentState.close(rowStreamInfo);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        rowStreamInfo.setCloseHandler(closeHandler);
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
