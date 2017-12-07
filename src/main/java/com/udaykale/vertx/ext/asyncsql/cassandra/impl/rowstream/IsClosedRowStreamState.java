package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

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
        throw new IllegalStateException("Cannot re-close when stream is already closed");
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        throw new IllegalStateException("Cannot execute when stream is already closed");
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        throw new IllegalStateException("Cannot pause when stream is already closed");
    }

    @Override
    public StateType type() {
        return StateType.CLOSED;
    }
}
