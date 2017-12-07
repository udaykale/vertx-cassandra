package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;

/**
 * @author uday
 */
final class RowStreamCloseHelper {

    private final SQLRowStream sqlRowStream;

    RowStreamCloseHelper(SQLRowStream sqlRowStream) {
        this.sqlRowStream = Objects.requireNonNull(sqlRowStream);
    }

    public void close(RowStreamInfo stateWrapper) {
        // change state to closing
        stateWrapper.setState(IsClosedRowStreamState.instance());
        stateWrapper.getAllRowStreams().remove(sqlRowStream);
        Handler<AsyncResult<Void>> closeHandler = stateWrapper.getCloseHandler();

        if (closeHandler != null) {
            stateWrapper.getWorkerExecutor().executeBlocking(future -> {
                try {
                    closeHandler.handle(null);
                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, futureResult -> {
                if (futureResult.failed()) {
                    stateWrapper.getWorkerExecutor().executeBlocking(future -> {
                        stateWrapper.getExceptionHandler().handle(futureResult.cause());
                        future.fail(futureResult.cause());
                    }, futureRes -> {
                        // do nothing since all the cases are handled in the blocking executor
                    });
                }
            });
        }  // do nothing in else part

    }
}
