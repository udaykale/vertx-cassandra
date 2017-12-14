package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
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

/**
 * @author uday
 */
final class CreatingConnectionClientState implements CassandraClientState {

    private CreatingConnectionClientState() {
    }

    static CreatingConnectionClientState instance() {
        return new CreatingConnectionClientState();
    }

    @Override
    public void close(ClientInfo clientInfo, Handler<AsyncResult<Void>> closeHandler) {
        clientInfo.setState(ClosedClientState.instance());

        clientInfo.getWorkerExecutor().executeBlocking((Future<Void> blockingFuture) -> {
            try {
                for (SQLConnection connection : clientInfo.getAllOpenConnections()) {
                    connection.close();
                }
                blockingFuture.complete();
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

            if (closeHandler != null) {
                Context context = clientInfo.getContext();
                context.runOnContext(action -> result.setHandler(closeHandler));
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
        int connectionId = clientInfo.generateConnectionId();

        // create a new connection
        CassandraConnection connection = CassandraConnectionImpl.of(connectionId, context,
                allOpenConnections, session, workerExecutor, preparedStatementCache);
        // add it to list instance ongoing connections
        clientInfo.addConnection(connection);

        Future<SQLConnection> result = Future.succeededFuture(connection);
        context.runOnContext(v -> handler.handle(result));
    }
}
