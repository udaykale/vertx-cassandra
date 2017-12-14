package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * @author uday
 */
public class RowStreamParameterErrorState implements RowStreamState {

    private RowStreamParameterErrorState() {
    }

    static RowStreamParameterErrorState instance() {
        return new RowStreamParameterErrorState();
    }

    @Override
    public void close(RowStreamInfo rowStreamInfo, CassandraRowStream cassandraRowStream,
                      Handler<AsyncResult<Void>> closeHandler) {
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void execute(RowStreamInfo rowStreamInfo) {
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void pause(RowStreamInfo rowStreamInfo) {
        // do nothing since an error was already thrown because parameters were set wrong
    }
}