package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

/**
 * @author uday
 */
final class IsExecutingRowStreamState implements RowStreamState {

    private final AtomicBoolean lock;

    private IsExecutingRowStreamState(AtomicBoolean lock) {
        this.lock = Objects.requireNonNull(lock);
    }

    static IsExecutingRowStreamState instance(AtomicBoolean lock) {
        return new IsExecutingRowStreamState(lock);
    }

    @Override
    public void close(RowStreamInfo rowStreamInfo, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        rowStreamInfo.setState(IsClosedRowStreamState.instance());
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot re-execute when stream is already executing");
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        rowStreamInfo.setState(IsPausedRowStreamState.instance(lock));
    }
}
