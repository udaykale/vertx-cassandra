package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.Objects;

/**
 * @author uday
 */
public final class CassandraClientImpl implements CassandraClient {

    private final String clientName;
    private final ClientInfo clientInfo;

    private CassandraClientImpl(String clientName, ClientInfo clientInfo) {
        this.clientName = Objects.requireNonNull(clientName);
        this.clientInfo = Objects.requireNonNull(clientInfo);
    }

    static CassandraClientImpl of(String clientName, Context context, Session session,
                                  WorkerExecutor workerExecutor) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(session);
        Objects.requireNonNull(clientName);
        Objects.requireNonNull(workerExecutor);

        CassandraClientState clientState = CreatingConnectionClientState.instance();
        ClientInfo clientInfo = ClientInfo.of(context, session, workerExecutor, clientState);
        return new CassandraClientImpl(clientName, clientInfo);
    }

    public static CassandraClient getOrCreateCassandraClient(Vertx vertx, Cluster cluster,
                                                             String keySpace, String clientName) {
        CassandraClientHelper cassandraClientHelper = CassandraClientHelper.instance(vertx);
        return cassandraClientHelper.getOrCreateCassandraClient(cluster, keySpace, clientName);
    }

    @Override
    public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        Objects.requireNonNull(handler);
        synchronized (this) {
            clientInfo.getState().createConnection(clientInfo, handler);
        }
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        clientInfo.setCloseHandler(closeHandler);
        close();
    }

    @Override
    public void close() {
        synchronized (this) {
            clientInfo.getState().close(clientInfo);
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
