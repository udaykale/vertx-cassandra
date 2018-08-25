package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.Objects;

final class CassandraClientImpl implements CassandraClient {

    private final Context context;
    private final String clientName;
    private final ClientStateWrapper clientStateWrapper;

    CassandraClientImpl(Context context, String clientName, ClientStateWrapper clientStateWrapper) {
        this.context = context;
        this.clientName = clientName;
        this.clientStateWrapper = clientStateWrapper;
    }

    @Override
    public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        Objects.requireNonNull(handler);
        synchronized (this) {
            context.runOnContext(v -> clientStateWrapper.createConnection(handler));
        }
        return this;
    }

    @Override
    public void close() {
        close(v -> Future.succeededFuture());
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        Objects.requireNonNull(closeHandler);
        synchronized (this) {
            context.runOnContext(v -> clientStateWrapper.close(closeHandler));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraClientImpl that = (CassandraClientImpl) o;
        return Objects.equals(clientName, that.clientName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientName);
    }
}
