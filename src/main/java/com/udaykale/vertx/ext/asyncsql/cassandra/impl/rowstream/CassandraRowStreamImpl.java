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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * @author uday
 */
public final class CassandraRowStreamImpl implements CassandraRowStream {

    private final Integer rowStreamId;
    private final ResultSet resultSet;
    private final RowStreamInfo rowStreamInfo;

    private List<String> columnNames;
    private Handler<AsyncResult<Void>> closeHandler;

    private CassandraRowStreamImpl(int rowStreamId, ResultSet resultSet, RowStreamInfo rowStreamInfo) {
        this.resultSet = resultSet;
        this.rowStreamId = rowStreamId;
        this.rowStreamInfo = rowStreamInfo;
    }

    public static CassandraRowStreamImpl of(int rowStreamId, ResultSet resultSet, WorkerExecutor workerExecutor,
                                            Set<SQLRowStream> allRowStreams, Function<Row, JsonArray> rowMapper) {
        Objects.requireNonNull(resultSet);
        Objects.requireNonNull(allRowStreams);
        Objects.requireNonNull(workerExecutor);

        int numColumns = resultSet.getColumnDefinitions().size();
        Function<Row, JsonArray> defaultRowMapper = defaultRowMapper(numColumns);
        RowStreamState state = IsPausedRowStreamState.instance(rowStreamId);
        Function<Row, JsonArray> finalRowMapper = Optional.ofNullable(rowMapper).orElse(defaultRowMapper);
        RowStreamInfo rowStreamInfo = RowStreamInfo.of(workerExecutor, allRowStreams, resultSet, state, finalRowMapper);

        return new CassandraRowStreamImpl(rowStreamId, resultSet, rowStreamInfo);
    }

    private static Function<Row, JsonArray> defaultRowMapper(int numColumns) {
        return row -> {
            JsonArray jsonArray = new JsonArray();

            for (int i = 0; i < numColumns; i++) {
                Object value = row.getObject(i);
                if (value instanceof String) {
                    jsonArray.add((String) value);
                } else if (value instanceof Integer) {
                    jsonArray.add((Integer) value);
                } else if (value instanceof Long) {
                    jsonArray.add((Long) value);
                } else if (value instanceof Float) {
                    jsonArray.add((Float) value);
                } else if (value instanceof Boolean) {
                    jsonArray.add((Boolean) value);
                } else {
                    jsonArray.add(value);
                }
            }

            return jsonArray;
        };
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
        synchronized (rowStreamId) {
            RowStreamState currentState = rowStreamInfo.getState();
            currentState.pause(rowStreamInfo);
        }
        return this;
    }

    @Override
    public SQLRowStream resume() {
        synchronized (rowStreamId) {
            RowStreamState currentState = rowStreamInfo.getState();
            currentState.execute(rowStreamInfo);
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
            synchronized (rowStreamId) {
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
        synchronized (rowStreamId) {
            RowStreamState currentState = rowStreamInfo.getState();
            currentState.close(rowStreamInfo, this, closeHandler);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
        close();
    }

    @Override
    public int getStreamId() {
        return rowStreamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraRowStreamImpl that = (CassandraRowStreamImpl) o;
        return rowStreamId.equals(that.rowStreamId);
    }

    @Override
    public int hashCode() {
        return rowStreamId;
    }
}
