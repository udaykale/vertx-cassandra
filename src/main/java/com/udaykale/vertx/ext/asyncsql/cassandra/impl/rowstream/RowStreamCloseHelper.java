package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * @author uday
 */
final class RowStreamCloseHelper {

    private RowStreamCloseHelper() {
    }

    static RowStreamCloseHelper of() {
        return new RowStreamCloseHelper();
    }

    public void close(RowStreamInfo rowStreamInfo, CassandraRowStream rowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        // change state to closing
        rowStreamInfo.setState(IsClosedRowStreamState.instance());
        rowStreamInfo.getAllRowStreams().remove(rowStream);

        if (closeHandler != null) {
            rowStreamInfo.getWorkerExecutor().executeBlocking(future -> {
                try {
                    closeHandler.handle(null);
                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, futureResult -> {
                if (futureResult.failed()) {
                    rowStreamInfo.getWorkerExecutor().executeBlocking(future -> {
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
