package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamState.StateType.*;

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
    public void close(RowStreamStateWrapper stateWrapper) {
        RowStreamCloseHelper rowStreamCloseHelper = new RowStreamCloseHelper(sqlRowStream);
        rowStreamCloseHelper.close(stateWrapper);
    }

    @Override
    public void execute(RowStreamStateWrapper stateWrapper) {
        // change the state to executing
        stateWrapper.setState(IsExecutingRowStreamState.instance(sqlRowStream));

        stateWrapper.getWorkerExecutor().executeBlocking(future -> readPage(stateWrapper, future),
                readPageResultFuture -> afterReadPage(stateWrapper, readPageResultFuture));
    }

    private static void readPage(RowStreamStateWrapper wrapper, Future<Object> future) {
        ResultSet resultSet = wrapper.getResultSet();
        int remainingInPage = resultSet.getAvailableWithoutFetching();

        try {
            int limit = wasLastPage(resultSet) ? 0 : 100;

            // read page only when we are in executing state and data is remaining in the page
            while (wrapper.getState().type() == EXECUTING && remainingInPage-- > limit) {
                Row row = resultSet.one();
                JsonArray jsonArray = wrapper.getRowMapper().apply(row);
                wrapper.getHandler().handle(jsonArray);
            }

            future.complete();
        } catch (Exception e) {
            // Any exception in result handler or while reading a page
            future.fail(e);
        }
    }

    private void afterReadPage(RowStreamStateWrapper stateWrapper, AsyncResult<Object> readPageResultFuture) {

        stateWrapper.getWorkerExecutor().executeBlocking(future -> {
            ResultSet resultSet = stateWrapper.getResultSet();

            try {
                if (readPageResultFuture.succeeded()) {
                    if (wasLastPage(resultSet)) {
                        // call end handler when all pages are read
                        stateWrapper.getEndHandler().handle(null);
                    } else {
                        synchronized (sqlRowStream) {
                            // pause the stream since we still have data and may continue executing
                            if (stateWrapper.getState().type() == EXECUTING) {
                                stateWrapper.setState(IsPausedRowStreamState.instance(sqlRowStream));
                                sqlRowStream.notify(); // leave the lock on wrapper
                                // call result set closed handler when a page is read
                                stateWrapper.getResultSetClosedHandler().handle(null);
                            } else {
                                // all other states (paused, closed) are not required to be handled
                            }
                        }
                    }
                    future.complete();
                } else {
                    // process the exception
                    stateWrapper.getExceptionHandler().handle(readPageResultFuture.cause());
                    future.fail(readPageResultFuture.cause());
                }
            } catch (Exception e) {
                stateWrapper.getExceptionHandler().handle(e);
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
    public void pause(RowStreamStateWrapper stateWrapper) {
        throw new IllegalStateException("Cannot re-pause when stream is already paused");
    }

    @Override
    public StateType type() {
        return PAUSED;
    }
}
