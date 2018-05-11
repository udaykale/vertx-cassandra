package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Row;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.function.Function;

/**
 * @author uday
 */
class ConnectionParameterErrorState implements CassandraConnectionState {

    private ConnectionParameterErrorState() {
    }

    static ConnectionParameterErrorState instance() {
        return new ConnectionParameterErrorState();
    }

    @Override
    public void close(ConnectionInfoWrapper connectionInfoWrapper, CassandraConnection connection,
                      Handler<AsyncResult<Void>> closeHandler) {
        // do nothing since an error was already thrown because parameters were set wrong
    }

    @Override
    public void stream(ConnectionInfoWrapper connectionInfoWrapper, List<String> queries, List<JsonArray> params,
                       Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler) {
        // do nothing since an error was already thrown because parameters were set wrong
    }
}
