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
    public void close(RowStreamInfo rowStreamInfo, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        RowStreamCloseHelper rowStreamCloseHelper = RowStreamCloseHelper.of();
        rowStreamCloseHelper.close(rowStreamInfo, cassandraRowStream, closeHandler);
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        // change the state to executing
        rowStreamInfo.setState(IsExecutingRowStreamState.instance(lock));

        rowStreamInfo.getWorkerExecutor().executeBlocking(future -> readPage(rowStreamInfo, future),
                readPageResultFuture -> afterReadPage(rowStreamInfo, readPageResultFuture));
    }

    private static void readPage(RowStreamInfo rowStreamInfo, Future<Object> future) {
        ResultSet resultSet = rowStreamInfo.getResultSet();
        int remainingInPage = resultSet.getAvailableWithoutFetching();

        try {
            int limit = wasLastPage(resultSet) ? 0 : 100;

            // read page only when we are in executing state and data is remaining in the page
            while (rowStreamInfo.getState().getClass() == IsExecutingRowStreamState.class
                    && remainingInPage-- > limit) {
                Row row = resultSet.one();
                JsonArray jsonArray = rowStreamInfo.getRowMapper().apply(row);
                if (rowStreamInfo.getHandler().isPresent()) {
                    rowStreamInfo.getHandler().get().handle(jsonArray);
                }
            }

            future.complete();
        } catch (Exception e) {
            // Any exception in result handler or while reading a page
            future.fail(e);
        }
    }

    private void afterReadPage(RowStreamInfo rowStreamInfo, AsyncResult<Object> readPageResultFuture) {

        rowStreamInfo.getWorkerExecutor().executeBlocking(future -> {
            ResultSet resultSet = rowStreamInfo.getResultSet();

            try {
                if (readPageResultFuture.failed()) {
                    throw new Exception(readPageResultFuture.cause());
                } else {
                    if (wasLastPage(resultSet)) {
                        // call end handler when all pages are read
                        if (rowStreamInfo.getEndHandler().isPresent()) {
                            rowStreamInfo.getEndHandler().get().handle(null);
                        }
                    } else {
                        synchronized (lock) {
                            // pause the stream since we still have data and may continue executing
                            if (rowStreamInfo.getState().getClass() == IsExecutingRowStreamState.class) {
                                rowStreamInfo.setState(IsPausedRowStreamState.instance(lock));
                                lock.notify(); // leave the lock on wrapper
                                // call result set closed handler when a page is read
                                if (rowStreamInfo.getResultSetClosedHandler().isPresent()) {
                                    rowStreamInfo.getResultSetClosedHandler().get().handle(null);
                                }
                            }  // all other states (paused, closed) are not required to be handled in else part
                        }
                    }
                    future.complete();
                }
            } catch (Exception e) {
                // process the exception
                if (rowStreamInfo.getExceptionHandler().isPresent()) {
                    rowStreamInfo.getExceptionHandler().get().handle(e);
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
    public void pause(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot re-pause when stream is already paused");
    }
}
