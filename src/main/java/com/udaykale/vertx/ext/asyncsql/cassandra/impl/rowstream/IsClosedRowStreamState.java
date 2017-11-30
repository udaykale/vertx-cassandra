package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

/**
 * @author uday
 */
final class IsClosedRowStreamState implements State<RowStreamStateWrapper> {

    private IsClosedRowStreamState() {
    }

    static IsClosedRowStreamState instance() {
        return new IsClosedRowStreamState();
    }

    @Override
    public void close(RowStreamStateWrapper stateWrapper) {
        throw new IllegalStateException("Cannot re-close when stream is already closed");
    }

    @Override
    public void execute(RowStreamStateWrapper stateWrapper) {
        throw new IllegalStateException("Cannot execute when stream is already closed");
    }

    @Override
    public void pause(RowStreamStateWrapper stateWrapper) {
        throw new IllegalStateException("Cannot pause when stream is already closed");
    }

    @Override
    public StateType type() {
        return StateType.IS_CLOSED;
    }
}
