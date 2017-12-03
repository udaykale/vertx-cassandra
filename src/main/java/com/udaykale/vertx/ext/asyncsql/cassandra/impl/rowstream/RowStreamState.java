package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

/**
 * @author uday
 */
public interface RowStreamState {

    void close(RowStreamStateWrapper stateWrapper);

    void execute(RowStreamStateWrapper stateWrapper);

    void pause(RowStreamStateWrapper stateWrapper);

    StateType type();

    enum StateType {
        EXECUTING, PAUSED, CLOSED;
    }
}
