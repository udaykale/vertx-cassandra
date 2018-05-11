package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

/**
 * @author uday
 */
final class IsClosedRowStreamState implements RowStreamState {

    private IsClosedRowStreamState() {
    }

    static IsClosedRowStreamState instance() {
        return new IsClosedRowStreamState();
    }

    @Override
    public void close(RowStreamInfoWrapper rowStreamInfoWrapper, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        handleIllegalStateException(rowStreamInfoWrapper, "Cannot re-close when stream is already closed");
    }

    @Override
    public void execute(RowStreamInfoWrapper rowStreamInfoWrapper) {
        handleIllegalStateException(rowStreamInfoWrapper, "Cannot execute when stream is already closed");
    }

    @Override
    public void pause(RowStreamInfoWrapper rowStreamInfoWrapper) {
        handleIllegalStateException(rowStreamInfoWrapper, "Cannot pause when stream is already closed");
    }
}
