package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

final class IsExecutingRowStreamState implements RowStreamState {

    private final AtomicBoolean lock;
    private final ResultSet resultSet;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams;
    private final Function<Row, JsonArray> rowMapper;

    private IsExecutingRowStreamState(AtomicBoolean lock, WorkerExecutor workerExecutor, ResultSet resultSet,
                                      Function<Row, JsonArray> rowMapper, Set<SQLRowStream> allRowStreams) {
        this.lock = Objects.requireNonNull(lock);
        this.resultSet = Objects.requireNonNull(resultSet);
        this.rowMapper = Objects.requireNonNull(rowMapper);
        this.allRowStreams = Objects.requireNonNull(allRowStreams);
        this.workerExecutor = Objects.requireNonNull(workerExecutor);
    }

    static IsExecutingRowStreamState instance(AtomicBoolean lock, WorkerExecutor workerExecutor, ResultSet resultSet,
                                              Function<Row, JsonArray> rowMapper, Set<SQLRowStream> allRowStreams) {
        return new IsExecutingRowStreamState(lock, workerExecutor, resultSet, rowMapper, allRowStreams);
    }

    @Override
    public void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler, Handler<Throwable> exceptionHandler) {
        rowStreamStateWrapper.setState(IsClosedRowStreamState.instance());
    }

    @Override
    public void execute(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler,
                        Handler<Void> endHandler, Handler<JsonArray> handler, Handler<Void> resultSetClosedHandler,
                        Handler<AsyncResult<Void>> closeHandler) {
        handleIllegalStateException(rowStreamStateWrapper, "Cannot re-execute when stream is already executing", exceptionHandler);
    }

    @Override
    public void pause(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler) {
        RowStreamState s = IsPausedRowStreamState.instance(lock, workerExecutor, resultSet, rowMapper, allRowStreams);
        rowStreamStateWrapper.setState(s);
    }
}
