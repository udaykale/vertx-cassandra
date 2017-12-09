package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import java.util.Objects;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.RowStreamUtil.handleIllegalStateException;

/**
 * @author uday
 */
final class IsExecutingRowStreamState implements RowStreamState {

    private final Integer rowStreamId;

    private IsExecutingRowStreamState(Integer rowStreamId) {
        this.rowStreamId = Objects.requireNonNull(rowStreamId);
    }

    static IsExecutingRowStreamState instance(Integer rowStreamId) {
        return new IsExecutingRowStreamState(rowStreamId);
    }

    @Override
    public void close(RowStreamInfo rowStreamInfo) {
        // TODO
        rowStreamInfo.setState(IsClosedRowStreamState.instance());
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        handleIllegalStateException(rowStreamInfo, "Cannot re-execute when stream is already executing");
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        rowStreamInfo.setState(IsPausedRowStreamState.instance(rowStreamId));
    }
}
