package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.Objects;

/**
 * @author uday
 */
final class RowStreamCloseHelper {

    private final Integer rowStreamId;

    RowStreamCloseHelper(Integer rowStreamId) {
        this.rowStreamId = Objects.requireNonNull(rowStreamId);
    }

    public void close(RowStreamInfo stateWrapper) {
        // change state to closing
        stateWrapper.setState(IsClosedRowStreamState.instance());
        stateWrapper.getAllRowStreams().remove(rowStreamId);

        if (stateWrapper.getCloseHandler().isPresent()) {
            Handler<AsyncResult<Void>> closeHandler = stateWrapper.getCloseHandler().get();
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
                        closeHandler.handle(null);
                        future.fail(futureResult.cause());
                    }, futureRes -> {
                        // do nothing since all the cases are handled in the blocking executor
                    });
                }
            });
        }  // do nothing in else part

    }
}
