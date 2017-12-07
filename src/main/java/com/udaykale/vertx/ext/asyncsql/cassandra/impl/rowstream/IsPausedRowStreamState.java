package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamState.StateType.EXECUTING;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamState.StateType.PAUSED;

/**
 * @author uday
 */
final class IsPausedRowStreamState implements RowStreamState {

    private final SQLRowStream sqlRowStream;

    private IsPausedRowStreamState(SQLRowStream sqlRowStream) {
        this.sqlRowStream = Objects.requireNonNull(sqlRowStream);
    }

    static IsPausedRowStreamState instance(SQLRowStream sqlRowStream) {
        Objects.requireNonNull(sqlRowStream);
        return new IsPausedRowStreamState(sqlRowStream);
    }

    @Override
    public void close(RowStreamInfo rowStreamInfo) {
        RowStreamCloseHelper rowStreamCloseHelper = new RowStreamCloseHelper(sqlRowStream);
        rowStreamCloseHelper.close(rowStreamInfo);
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        // change the state to executing
        rowStreamInfo.setState(IsExecutingRowStreamState.instance(sqlRowStream));

        rowStreamInfo.getWorkerExecutor().executeBlocking(future -> readPage(rowStreamInfo, future),
                readPageResultFuture -> afterReadPage(rowStreamInfo, readPageResultFuture));
    }

    private static void readPage(RowStreamInfo rowStreamInfo, Future<Object> future) {
        ResultSet resultSet = rowStreamInfo.getResultSet();
        int remainingInPage = resultSet.getAvailableWithoutFetching();

        try {
            int limit = wasLastPage(resultSet) ? 0 : 100;

            // read page only when we are in executing state and data is remaining in the page
            while (rowStreamInfo.getState().type() == EXECUTING && remainingInPage-- > limit) {
                Row row = resultSet.one();
                JsonArray jsonArray = rowStreamInfo.getRowMapper().apply(row);
                rowStreamInfo.getHandler().handle(jsonArray);
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
                        rowStreamInfo.getEndHandler().handle(null);
                    } else {
                        synchronized (sqlRowStream) {
                            // pause the stream since we still have data and may continue executing
                            if (rowStreamInfo.getState().type() == EXECUTING) {
                                rowStreamInfo.setState(IsPausedRowStreamState.instance(sqlRowStream));
                                sqlRowStream.notify(); // leave the lock on wrapper
                                // call result set closed handler when a page is read
                                rowStreamInfo.getResultSetClosedHandler().handle(null);
                            }  // all other states (paused, closed) are not required to be handled in else part

                        }
                    }
                    future.complete();
                }
            } catch (Exception e) {
                // process the exception
                rowStreamInfo.getExceptionHandler().handle(e);
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
        throw new IllegalStateException("Cannot re-pause when stream is already paused");
    }

    @Override
    public StateType type() {
        return PAUSED;
    }
}
