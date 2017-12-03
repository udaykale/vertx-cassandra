package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientState.StateType.CREATING_CONNECTION;

/**
 * @author uday
 */
public final class CassandraClientImpl implements CassandraClient {

    private final Context context;
    private final CassandraClientStateWrapper stateWrapper;

    CassandraClientImpl(Context context, Session session, WorkerExecutor workerExecutor) {
        stateWrapper = CassandraClientStateWrapper.builder()
                .withConnectionIdGenerator(new AtomicInteger(1))
                .withSession(session)
                .withContext(context)
                .withWorkerExecutor(workerExecutor)
                .withPreparedStatementCache(new ConcurrentHashMap<>())
                .withCurrentCassandraClientState(CreatingConnectionClientState.instance(this))
                .withAllOpenConnections(new ConcurrentSkipListSet<>())
                .build();
        this.context = context;
    }

    public static CassandraClient getOrCreateCassandraClient(Vertx vertx, Cluster cluster,
                                                             String keySpace, String clientName) {
        CassandraClientHelper cassandraClientHelper = new CassandraClientHelper(vertx);
        return cassandraClientHelper.getOrCreateCassandraClient(cluster, keySpace, clientName);
    }

    @Override
    public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        synchronized (this) {
            CassandraClientState currentState = stateWrapper.getCurrentCassandraClientState();

            // check if connection can be created
            if (currentState.type() == CREATING_CONNECTION) {
                Future<SQLConnection> result = currentState.createConnection(stateWrapper);
                context.runOnContext(action -> result.setHandler(handler));
            } else {
                Exception e = new IllegalStateException("Cannot create a connection when client is closed");
                Future<SQLConnection> result = Future.failedFuture(e);
                // connection is doClose/setClosing
                context.runOnContext(action -> result.setHandler(handler));
            }
        }
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        stateWrapper.setCloseHandler(closeHandler);
        close();
    }

    @Override
    public void close() {
        synchronized (this) {
            CassandraClientState currentState = stateWrapper.getCurrentCassandraClientState();

            if (currentState.type() == CREATING_CONNECTION) {
                currentState.close(stateWrapper);
            } else {
                // Connection already closed nothing, needed to be done
            }
        }
    }
}
