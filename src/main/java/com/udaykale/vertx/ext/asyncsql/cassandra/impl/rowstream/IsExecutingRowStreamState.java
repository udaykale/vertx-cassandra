package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.ext.sql.SQLRowStream;

import java.util.Objects;

/**
 * @author uday
 */
final class IsExecutingRowStreamState implements RowStreamState {

    private final SQLRowStream sqlRowStream;

    private IsExecutingRowStreamState(SQLRowStream sqlRowStream) {
        this.sqlRowStream = Objects.requireNonNull(sqlRowStream);
    }

    static IsExecutingRowStreamState instance(SQLRowStream sqlRowStream) {
        return new IsExecutingRowStreamState(sqlRowStream);
    }

    @Override
    public void close(RowStreamStateWrapper stateWrapper) {
        stateWrapper.setState(IsClosedRowStreamState.instance());
    }

    @Override
    public void execute(RowStreamStateWrapper stateWrapper) {
        throw new IllegalStateException("Cannot re-execute when stream is already executing");
    }

    @Override
    public void pause(RowStreamStateWrapper stateWrapper) {
        stateWrapper.setState(IsPausedRowStreamState.instance(sqlRowStream));
    }

    @Override
    public StateType type() {
        return StateType.EXECUTING;
    }
}
