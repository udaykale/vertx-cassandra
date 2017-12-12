package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraRowStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * @author uday
 */
public interface RowStreamState {

    void close(RowStreamInfo rowStreamInfo, CassandraRowStream cassandraRowStream,
               Handler<AsyncResult<Void>> closeHandler);

    void execute(RowStreamInfo rowStreamInfo);

    void pause(RowStreamInfo rowStreamInfo);
}
