package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

final class IsPausedRowStreamState implements RowStreamState {

    private final AtomicBoolean lock;
    private final ResultSet resultSet;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams;
    private final Function<Row, JsonArray> rowMapper;

    private IsPausedRowStreamState(AtomicBoolean lock, WorkerExecutor workerExecutor, ResultSet resultSet,
                                   Set<SQLRowStream> sqlRowStream, Function<Row, JsonArray> rowMapper) {
        this.lock = lock;
        this.resultSet = resultSet;
        this.rowMapper = rowMapper;
        allRowStreams = sqlRowStream;
        this.workerExecutor = workerExecutor;
    }

    static IsPausedRowStreamState instance(AtomicBoolean lock, WorkerExecutor workerExecutor, ResultSet resultSet,
                                           Function<Row, JsonArray> rowMapper, Set<SQLRowStream> allRowStreams) {
        return new IsPausedRowStreamState(lock, workerExecutor, resultSet, allRowStreams, rowMapper);
    }

    @Override
    public void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler, Handler<Throwable> exceptionHandler) {
        RowStreamCloseHelper rowStreamCloseHelper = RowStreamCloseHelper.of(allRowStreams, workerExecutor);
        rowStreamCloseHelper.close(rowStreamStateWrapper, cassandraRowStream, closeHandler);
    }

    @Override
    public void execute(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler,
                        Handler<Void> endHandler, Handler<JsonArray> handler, Handler<Void> resultSetClosedHandler,
                        Handler<AsyncResult<Void>> closeHandler) {
        // change the state to executing
        RowStreamState state = IsExecutingRowStreamState.instance(lock, workerExecutor, resultSet, rowMapper, allRowStreams);
        rowStreamStateWrapper.setState(state);

        workerExecutor.executeBlocking(future -> readPage(rowStreamStateWrapper, future, handler),
                readPageResultFuture -> afterReadPage(rowStreamStateWrapper, readPageResultFuture, endHandler,
                        exceptionHandler, resultSetClosedHandler));
    }

    private void readPage(RowStreamStateWrapper rowStreamStateWrapper, Future<Object> future,
                          Handler<JsonArray> handler) {
        int remainingInPage = resultSet.getAvailableWithoutFetching();

        try {
            int limit = wasLastPage(resultSet) ? 0 : 100;

            // read page only when we are in executing state and data is remaining in the page
            while (rowStreamStateWrapper.stateClass() == IsExecutingRowStreamState.class
                    && remainingInPage-- > limit) {
                Row row = resultSet.one();
                JsonArray jsonArray = rowMapper.apply(row);
                if (handler != null) {
                    handler.handle(jsonArray);
                }
            }

            future.complete();
        } catch (Exception e) {
            // Any exception in result handler or while reading a page
            future.fail(e);
        }
    }

    private void afterReadPage(RowStreamStateWrapper rowStreamStateWrapper, AsyncResult<Object> readPageResultFuture,
                               Handler<Void> endHandler, Handler<Throwable> exceptionHandler,
                               Handler<Void> resultSetClosedHandler) {

        workerExecutor.executeBlocking(future -> {
            try {
                if (readPageResultFuture.failed()) {
                    throw new Exception(readPageResultFuture.cause());
                } else {
                    if (wasLastPage(resultSet)) {
                        // call end handler when all pages are read
                        if (endHandler != null) {
                            endHandler.handle(null);
                        }
                    } else {
                        synchronized (lock) {
                            // pause the stream since we still have data and may continue executing
                            if (rowStreamStateWrapper.stateClass() == IsExecutingRowStreamState.class) {
                                rowStreamStateWrapper.setState(IsPausedRowStreamState.instance(lock, workerExecutor,
                                        resultSet, rowMapper, allRowStreams));
                                lock.notify(); // leave the lock on wrapper
                                // call result set closed handler when a page is read
                                if (resultSetClosedHandler != null) {
                                    resultSetClosedHandler.handle(null);
                                }
                            }  // all other states (paused, closed) are not required to be handled in else part
                        }
                    }
                    future.complete();
                }
            } catch (Exception e) {
                // process the exception
                if (exceptionHandler != null) {
                    exceptionHandler.handle(e);
                }
                future.fail(e);
            }
        }, afterReadPageFutureResult -> {
            // do nothing since all the cases are handled in the blocking executor
        });
    }

    private static boolean wasLastPage(ResultSet resultSet) {
        return resultSet.getExecutionInfo().getPagingState() == null;
    }

    @Override
    public void pause(RowStreamStateWrapper rowStreamStateWrapper, Handler<Throwable> exceptionHandler) {
        handleIllegalStateException(rowStreamStateWrapper, "Cannot re-pause when stream is already paused", exceptionHandler);
    }
}
