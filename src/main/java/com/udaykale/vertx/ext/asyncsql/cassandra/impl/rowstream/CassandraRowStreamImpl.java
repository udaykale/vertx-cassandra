package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public final class CassandraRowStreamImpl implements CassandraRowStream {

    private final int rowStreamId;
    private final Context context;
    private final AtomicBoolean lock;
    private final ResultSet resultSet;
    private final RowStreamStateWrapper rowStreamStateWrapper;

    private Handler<Void> endHandler;
    private Handler<JsonArray> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> resultSetClosedHandler;
    private Handler<AsyncResult<Void>> closeHandler;


    private List<String> columnNames;

    private CassandraRowStreamImpl(int rowStreamId, ResultSet resultSet, RowStreamStateWrapper rowStreamStateWrapper,
                                   AtomicBoolean lock, Context context) {
        this.lock = lock;
        this.context = context;
        this.resultSet = resultSet;
        this.rowStreamId = rowStreamId;
        this.rowStreamStateWrapper = rowStreamStateWrapper;
    }

    public static CassandraRowStreamImpl of(int rowStreamId, ResultSet resultSet, WorkerExecutor workerExecutor,
                                            Set<SQLRowStream> allRowStreams, Function<Row, JsonArray> rowMapper,
                                            Context context) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(resultSet);
        Objects.requireNonNull(allRowStreams);
        Objects.requireNonNull(workerExecutor);

        AtomicBoolean lock = new AtomicBoolean();
        int numColumns = resultSet.getColumnDefinitions().size();
        Function<Row, JsonArray> finalRowMapper = Optional.ofNullable(rowMapper).orElse(defaultRowMapper(numColumns));
        RowStreamState state = IsPausedRowStreamState.instance(lock, workerExecutor, resultSet, finalRowMapper, allRowStreams);
        RowStreamStateWrapper rowStreamStateWrapper = RowStreamStateWrapper.of(state);

        return new CassandraRowStreamImpl(rowStreamId, resultSet, rowStreamStateWrapper, lock, context);
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
        if (exceptionHandler == null) {
            Throwable error = new NullPointerException("Exception handler cannot be null");
            rowStreamStateWrapper.setState(ParamErrorRowStreamState.instance(error));
        } else {
            this.exceptionHandler = exceptionHandler;
        }
        return this;
    }

    @Override
    public SQLRowStream handler(Handler<JsonArray> handler) {
        if (handler == null) {
            Throwable error = new NullPointerException("Stream handler cannot be null");
            rowStreamStateWrapper.setState(ParamErrorRowStreamState.instance(error));
        } else {
            this.handler = handler;
        }
        return resume();
    }

    @Override
    public SQLRowStream endHandler(Handler<Void> endHandler) {
        if (endHandler == null) {
            synchronized (lock) {
                Throwable error = new NullPointerException("End handler cannot be null");
                rowStreamStateWrapper.setState(ParamErrorRowStreamState.instance(error));
            }
        } else {
            this.endHandler = endHandler;
        }
        return this;
    }

    @Override
    public SQLRowStream resultSetClosedHandler(Handler<Void> resultSetClosedHandler) {
        if (resultSetClosedHandler == null) {
            synchronized (lock) {
                Throwable throwable = new NullPointerException("Result Set closed handler cannot be null");
                rowStreamStateWrapper.setState(ParamErrorRowStreamState.instance(throwable));
            }
        } else {
            this.resultSetClosedHandler = resultSetClosedHandler;
        }
        return this;
    }

    @Override
    public SQLRowStream pause() {
        synchronized (lock) {
            context.runOnContext(v -> rowStreamStateWrapper.pause(exceptionHandler));
        }
        return this;
    }

    @Override
    public SQLRowStream resume() {
        synchronized (lock) {
            context.runOnContext(v -> rowStreamStateWrapper.execute(exceptionHandler, endHandler, handler, resultSetClosedHandler, closeHandler));
        }
        return this;
    }

    @Override
    public int column(String name) {
        return resultSet.getColumnDefinitions().getIndexOf(name) - 1;
    }

    @Override
    public List<String> columns() {
        if (columnNames == null) {
            synchronized (lock) {
                columnNames = resultSet.getColumnDefinitions().asList().stream()
                        .map(ColumnDefinitions.Definition::getName)
                        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
            }
        } else {
            // no need for else
        }

        return columnNames;
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
        synchronized (lock) {
            context.runOnContext(v -> rowStreamStateWrapper.close(this, closeHandler, exceptionHandler));
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
        return rowStreamId == that.rowStreamId;
    }

    @Override
    public int hashCode() {
        return rowStreamId;
    }
}
