package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Set;

final class RowStreamCloseHelper {

    private final Set<SQLRowStream> allRowStreams;
    private final WorkerExecutor workerExecutor;

    private RowStreamCloseHelper(Set<SQLRowStream> allRowStreams, WorkerExecutor workerExecutor) {
        this.allRowStreams = allRowStreams;
        this.workerExecutor = workerExecutor;
    }

    static RowStreamCloseHelper of(Set<SQLRowStream> allRowStreams, WorkerExecutor workerExecutor) {
        return new RowStreamCloseHelper(allRowStreams, workerExecutor);
    }

    public void close(RowStreamStateWrapper rowStreamStateWrapper, CassandraRowStream rowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        // change state to closing
        rowStreamStateWrapper.setState(IsClosedRowStreamState.instance());
        allRowStreams.remove(rowStream);

        if (closeHandler != null) {
            workerExecutor.executeBlocking(future -> {
                try {
                    closeHandler.handle(null);
                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, futureResult -> {
                if (futureResult.failed()) {
                    workerExecutor.executeBlocking(future -> {
                        closeHandler.handle(null);
                        future.fail(futureResult.cause());
                    }, futureRes -> {
                        // do nothing since all the cases are handled in the blocking executor
                    });
                }
            });
        } else {
            // do nothing in else part
        }
    }
}
