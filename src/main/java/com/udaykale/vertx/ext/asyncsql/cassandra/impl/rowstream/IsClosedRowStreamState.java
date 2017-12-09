package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

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
    public void close(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot re-close when stream is already closed");
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot execute when stream is already closed");
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot pause when stream is already closed");
    }
}
