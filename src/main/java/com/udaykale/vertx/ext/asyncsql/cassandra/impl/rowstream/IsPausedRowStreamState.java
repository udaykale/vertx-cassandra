package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;

import java.util.Objects;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

/**
 * @author uday
 */
final class IsPausedRowStreamState implements RowStreamState {

    private final Integer rowStreamId;

    private IsPausedRowStreamState(Integer rowStreamId) {
        this.rowStreamId = Objects.requireNonNull(rowStreamId);
    }

    static IsPausedRowStreamState instance(Integer rowStreamId) {
        Objects.requireNonNull(rowStreamId);
        return new IsPausedRowStreamState(rowStreamId);
    }

    @Override
    public void close(RowStreamInfo rowStreamInfo) {
        RowStreamCloseHelper rowStreamCloseHelper = new RowStreamCloseHelper(rowStreamId);
        rowStreamCloseHelper.close(rowStreamInfo);
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        // change the state to executing
        rowStreamInfo.setState(IsExecutingRowStreamState.instance(rowStreamId));

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
                        synchronized (rowStreamId) {
                            // pause the stream since we still have data and may continue executing
                            if (rowStreamInfo.getState().getClass() == IsExecutingRowStreamState.class) {
                                rowStreamInfo.setState(IsPausedRowStreamState.instance(rowStreamId));
                                rowStreamId.notify(); // leave the lock on wrapper
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
