package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * @author uday
 */
public final class CassandraRowStream implements SQLRowStream {

    private final ResultSet resultSet;
    private final List<String> columns;
    private final RowStreamStateWrapper.Builder stateWrapperBuilder;

    private RowStreamStateWrapper stateWrapper;

    public CassandraRowStream(ResultSet resultSet, WorkerExecutor workerExecutor,
                              Set<SQLRowStream> allRowStreams,
                              Function<Row, JsonArray> rowMapper) {
        this.resultSet = resultSet;
        this.columns = columns(resultSet);
        this.stateWrapperBuilder = stateWrapper(workerExecutor, allRowStreams, rowMapper);
    }

    private List<String> columns(ResultSet resultSet) {
        return resultSet.getColumnDefinitions()
                .asList().stream()
                .map(ColumnDefinitions.Definition::getName)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
    }

    private RowStreamStateWrapper.Builder stateWrapper(WorkerExecutor workerExecutor,
                                                       Set<SQLRowStream> allRowStreams,
                                                       Function<Row, JsonArray> rowMapper) {
        State<RowStreamStateWrapper> currentState = IsPausedRowStreamState.instance(this);
        Function<Row, JsonArray> defaultRowMapper = defaultRowMapper(this.columns);
        Function<Row, JsonArray> finalRowMapper = Optional.ofNullable(rowMapper).orElse(defaultRowMapper);
        return RowStreamStateWrapper.builder(currentState)
                .withRowMapper(finalRowMapper)
                .withWorkerExecutor(workerExecutor)
                .withAllRowStreams(allRowStreams)
                .withResultSet(this.resultSet);
    }

    @Override
    public SQLRowStream exceptionHandler(Handler<Throwable> exceptionHandler) {
        stateWrapperBuilder.withExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public SQLRowStream handler(Handler<JsonArray> handler) {
        stateWrapper = stateWrapperBuilder.withHandler(handler).build();
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

    private static Function<Row, JsonArray> defaultRowMapper(List<String> columns) {
        return row -> {
            JsonArray jsonArray = new JsonArray();

            for (String column : columns) {
                Object value = row.getObject(column);
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
    public SQLRowStream endHandler(Handler<Void> endHandler) {
        stateWrapperBuilder.withEndHandler(endHandler);
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
        stateWrapperBuilder.withResultSetClosedHandler(resultSetClosedHandler);
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
                stateWrapper.getState().execute(stateWrapper);
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
        stateWrapperBuilder.withCloseHandler(closeHandler);
    }
}
