package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLConnection;

import java.util.Map;
import java.util.Set;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientState.StateType.CREATING_CONNECTION;

/**
 * @author uday
 */
final class CreatingConnectionClientState implements CassandraClientState {

    private final CassandraClient cassandraClient;

    private CreatingConnectionClientState(CassandraClient cassandraClient) {
        this.cassandraClient = cassandraClient;
    }

    static CreatingConnectionClientState instance(CassandraClient cassandraClient) {
        return new CreatingConnectionClientState(cassandraClient);
    }

    @Override
    public void close(CassandraClientStateWrapper stateWrapper) {
        WorkerExecutor workerExecutor = stateWrapper.getWorkerExecutor();
        stateWrapper.setState(ClosedClientState.instance());

        workerExecutor.executeBlocking(blockingFuture -> {
            try {
                synchronized (cassandraClient) {
                    cassandraClient.notify();
                    Set<CassandraConnection> cassandraConnections = stateWrapper.getAllOpenConnections();
                    for (CassandraConnection cassandraConnection : cassandraConnections) {
                        cassandraConnection.close();
                    }
                    blockingFuture.complete();
                }
            } catch (Exception e) {
                blockingFuture.fail(e);
            }
        }, blockingFuture -> {
            Future<Void> result = Future.future();

            if (blockingFuture.failed()) {
                result.fail(blockingFuture.cause());
            } else {
                result.complete();
            }

            Handler<AsyncResult<Void>> closeHandler = stateWrapper.getCloseHandler();
            Context context = stateWrapper.getContext();
            context.runOnContext(action -> result.setHandler(closeHandler));
        });
    }

    @Override
    public Future<SQLConnection> createConnection(CassandraClientStateWrapper stateWrapper) {
        Future<SQLConnection> result = Future.future();
        Context context = stateWrapper.getContext();
        Session session = stateWrapper.getSession();
        WorkerExecutor workerExecutor = stateWrapper.getWorkerExecutor();
        Set<CassandraConnection> allOpenConnections = stateWrapper.getAllOpenConnections();
        Map<String, PreparedStatement> preparedStatementCache = stateWrapper.getPreparedStatementCache();

        // create a new connection
        int connectionId = stateWrapper.generateConnectionId();
        // add it to list instance ongoing connections
        CassandraConnection connection = new CassandraConnectionImpl(connectionId, context,
                allOpenConnections, session, workerExecutor, preparedStatementCache);
        stateWrapper.addConnection(connection);
        result.complete(connection);

        return result;
    }

    @Override
    public StateType type() {
        return CREATING_CONNECTION;
    }
}
