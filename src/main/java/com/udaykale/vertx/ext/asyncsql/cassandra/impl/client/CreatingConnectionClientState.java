package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLConnection;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * State of the client when it has is accepting request for new connection creation
 *
 * @author uday
 */
final class CreatingConnectionClientState implements CassandraClientState {

    private final Map<String, PreparedStatement> preparedStatementsCache;
    private final Set<CassandraConnection> allOpenConnections;
    private final AtomicInteger connectionIdGenerator;
    private final WorkerExecutor workerExecutor;
    private final Context context;
    private final Session session;

    private CreatingConnectionClientState(Context context, Session session, WorkerExecutor workerExecutor,
                                          Map<String, PreparedStatement> preparedStatementsCache,
                                          Set<CassandraConnection> allOpenConnections,
                                          AtomicInteger connectionIdGenerator) {
        this.preparedStatementsCache = preparedStatementsCache;
        this.connectionIdGenerator = connectionIdGenerator;
        this.allOpenConnections = allOpenConnections;
        this.workerExecutor = workerExecutor;
        this.context = context;
        this.session = session;
    }

    static CreatingConnectionClientState instance(Vertx vertx, Cluster cluster, String keySpace, String clientName) {

        Context context = vertx.getOrCreateContext();
        AtomicInteger connectionIdGenerator = new AtomicInteger(1);
        Set<CassandraConnection> allOpenConnection = new ConcurrentSkipListSet<>();
        WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor(clientName);
        Map<String, PreparedStatement> preparedStatementsCache = new ConcurrentHashMap<>();
        Session session = keySpace.isEmpty() ? cluster.connect() : cluster.connect(keySpace);

        return new CreatingConnectionClientState(context, session, workerExecutor,
                preparedStatementsCache, allOpenConnection, connectionIdGenerator);
    }

    @Override
    public void close(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<Void>> closeHandler) {
        clientStateWrapper.setState(ClosedClientState.instance(context));
        Iterator<CassandraConnection> connectionIterator = allOpenConnections.iterator();

        workerExecutor.executeBlocking((Future<Void> blockingFuture) ->
                        closeAllConnections(connectionIterator, blockingFuture),
                blockingResultFuture -> afterBlockingExecution(closeHandler, blockingResultFuture));
    }

    private void afterBlockingExecution(Handler<AsyncResult<Void>> closeHandler, AsyncResult<Void> blockingResultFuture) {
        Future<Void> result = Future.future();

        if (blockingResultFuture.failed()) {
            result.fail(blockingResultFuture.cause());
        } else {
            result.complete();
        }

        if (closeHandler != null) {
            context.runOnContext(action -> result.setHandler(closeHandler));
        } else {
            // nothing to do in else part
        }
    }

    private void closeAllConnections(Iterator<CassandraConnection> connectionIterator, Future<Void> blockingFuture) {
        if (connectionIterator.hasNext()) {
            SQLConnection connection = connectionIterator.next();

            connection.close(resultFuture -> {
                if (resultFuture.failed()) {
                    blockingFuture.fail(resultFuture.cause());
                } else {
                    closeAllConnections(connectionIterator, blockingFuture);
                }
            });
        } else {
            blockingFuture.complete();
        }
    }

    @Override
    public void createConnection(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<SQLConnection>> handler) {

        int connectionId = connectionIdGenerator.getAndIncrement();
        // create a new connection
        CassandraConnection connection = CassandraConnectionImpl.of(connectionId, context,
                allOpenConnections, session, workerExecutor, preparedStatementsCache);
        // add it to list instance ongoing connections
        allOpenConnections.add(connection);

        Future<SQLConnection> result = Future.succeededFuture(connection);
        context.runOnContext(v -> handler.handle(result));
    }
}
