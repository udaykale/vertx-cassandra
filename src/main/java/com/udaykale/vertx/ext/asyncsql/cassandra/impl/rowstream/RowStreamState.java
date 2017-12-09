package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

/**
 * @author uday
 */
public interface RowStreamState {

    void close(RowStreamInfo rowStreamInfo);

    void execute(RowStreamInfo rowStreamInfo);

    void pause(RowStreamInfo rowStreamInfo);
}
