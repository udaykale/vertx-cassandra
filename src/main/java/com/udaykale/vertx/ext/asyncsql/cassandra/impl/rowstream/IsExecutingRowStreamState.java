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
    public void close(RowStreamInfo rowStreamInfo) {
        rowStreamInfo.setState(IsClosedRowStreamState.instance());
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        throw new IllegalStateException("Cannot re-execute when stream is already executing");
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        rowStreamInfo.setState(IsPausedRowStreamState.instance(sqlRowStream));
    }

    @Override
    public StateType type() {
        return StateType.EXECUTING;
    }
}
