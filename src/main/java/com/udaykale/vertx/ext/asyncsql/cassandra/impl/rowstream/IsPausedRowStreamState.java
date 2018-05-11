package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

/**
 * @author uday
 */
final class IsPausedRowStreamState implements RowStreamState {

    private final AtomicBoolean lock;

    private IsPausedRowStreamState(AtomicBoolean lock) {
        this.lock = Objects.requireNonNull(lock);
    }

    static IsPausedRowStreamState instance(AtomicBoolean lock) {
        Objects.requireNonNull(lock);
        return new IsPausedRowStreamState(lock);
    }

    @Override
    public void close(RowStreamInfoWrapper rowStreamInfoWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        RowStreamCloseHelper rowStreamCloseHelper = RowStreamCloseHelper.of();
        rowStreamCloseHelper.close(rowStreamInfoWrapper, cassandraRowStream, closeHandler);
    }

    @Override
    public void execute(RowStreamInfoWrapper rowStreamInfoWrapper) {
        // change the state to executing
        rowStreamInfoWrapper.setState(IsExecutingRowStreamState.instance(lock));

        rowStreamInfoWrapper.getWorkerExecutor().executeBlocking(future -> readPage(rowStreamInfoWrapper, future),
                readPageResultFuture -> afterReadPage(rowStreamInfoWrapper, readPageResultFuture));
    }

    private static void readPage(RowStreamInfoWrapper rowStreamInfoWrapper, Future<Object> future) {
        ResultSet resultSet = rowStreamInfoWrapper.getResultSet();
        int remainingInPage = resultSet.getAvailableWithoutFetching();

        try {
            int limit = wasLastPage(resultSet) ? 0 : 100;

            // read page only when we are in executing state and data is remaining in the page
            while (rowStreamInfoWrapper.stateClass() == IsExecutingRowStreamState.class
                    && remainingInPage-- > limit) {
                Row row = resultSet.one();
                JsonArray jsonArray = rowStreamInfoWrapper.getRowMapper().apply(row);
                if (rowStreamInfoWrapper.getHandler().isPresent()) {
                    rowStreamInfoWrapper.getHandler().get().handle(jsonArray);
                }
            }

            future.complete();
        } catch (Exception e) {
            // Any exception in result handler or while reading a page
            future.fail(e);
        }
    }

    private void afterReadPage(RowStreamInfoWrapper rowStreamInfoWrapper, AsyncResult<Object> readPageResultFuture) {

        rowStreamInfoWrapper.getWorkerExecutor().executeBlocking(future -> {
            ResultSet resultSet = rowStreamInfoWrapper.getResultSet();

            try {
                if (readPageResultFuture.failed()) {
                    throw new Exception(readPageResultFuture.cause());
                } else {
                    if (wasLastPage(resultSet)) {
                        // call end handler when all pages are read
                        if (rowStreamInfoWrapper.getEndHandler().isPresent()) {
                            rowStreamInfoWrapper.getEndHandler().get().handle(null);
                        }
                    } else {
                        synchronized (lock) {
                            // pause the stream since we still have data and may continue executing
                            if (rowStreamInfoWrapper.stateClass() == IsExecutingRowStreamState.class) {
                                rowStreamInfoWrapper.setState(IsPausedRowStreamState.instance(lock));
                                lock.notify(); // leave the lock on wrapper
                                // call result set closed handler when a page is read
                                if (rowStreamInfoWrapper.getResultSetClosedHandler().isPresent()) {
                                    rowStreamInfoWrapper.getResultSetClosedHandler().get().handle(null);
                                }
                            }  // all other states (paused, closed) are not required to be handled in else part
                        }
                    }
                    future.complete();
                }
            } catch (Exception e) {
                // process the exception
                if (rowStreamInfoWrapper.getExceptionHandler().isPresent()) {
                    rowStreamInfoWrapper.getExceptionHandler().get().handle(e);
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
    public void pause(RowStreamInfoWrapper rowStreamInfoWrapper) {
        handleIllegalStateException(rowStreamInfoWrapper, "Cannot re-pause when stream is already paused");
    }
}
