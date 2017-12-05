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
import java.util.Optional;
import java.util.Set;

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
    public void close(ClientInfo clientInfo) {
        WorkerExecutor workerExecutor = clientInfo.getWorkerExecutor();
        clientInfo.setState(ClosedClientState.instance());

        workerExecutor.executeBlocking((Future<Void> blockingFuture) -> {
            try {
                synchronized (cassandraClient) {
                    cassandraClient.notify();
                    clientInfo.closeAllOpenConnections();
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

            Optional<Handler<AsyncResult<Void>>> closeHandler = clientInfo.getCloseHandler();
            if (closeHandler.isPresent()) {
                Context context = clientInfo.getContext();
                context.runOnContext(action -> result.setHandler(closeHandler.get()));
            } // nothing to do in else part
        });
    }

    @Override
    public void createConnection(ClientInfo clientInfo, Handler<AsyncResult<SQLConnection>> handler) {
        Context context = clientInfo.getContext();
        Session session = clientInfo.getSession();
        WorkerExecutor workerExecutor = clientInfo.getWorkerExecutor();
        Set<CassandraConnection> allOpenConnections = clientInfo.getAllOpenConnections();
        Map<String, PreparedStatement> preparedStatementCache = clientInfo.getPreparedStatementCache();

        // create a new connection
        int connectionId = clientInfo.generateConnectionId();
        // add it to list instance ongoing connections
        CassandraConnection connection = new CassandraConnectionImpl(connectionId, context,
                allOpenConnections, session, workerExecutor, preparedStatementCache);
        clientInfo.addConnection(connection);

        Future<SQLConnection> result = Future.succeededFuture(connection);
        context.runOnContext(v -> handler.handle(result));
    }
}
