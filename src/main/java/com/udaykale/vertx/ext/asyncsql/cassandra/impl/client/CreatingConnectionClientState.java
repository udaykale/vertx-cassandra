package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLConnection;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * State of the client when it has is accepting request for new connection creation
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

    static CreatingConnectionClientState instance(Context context, Cluster cluster, String keySpace,
                                                  WorkerExecutor workerExecutor) {

        AtomicInteger connectionIdGenerator = new AtomicInteger(1);
        Set<CassandraConnection> allOpenConnection = new ConcurrentSkipListSet<>();
        Map<String, PreparedStatement> preparedStatementsCache = new ConcurrentHashMap<>();
        Session session = keySpace.isEmpty() ? cluster.connect() : cluster.connect(keySpace);

        return new CreatingConnectionClientState(context, session, workerExecutor,
                preparedStatementsCache, allOpenConnection, connectionIdGenerator);
    }

    @Override
    public void close(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<Void>> closeHandler) {
        clientStateWrapper.setState(ClosedClientState.instance());

        List<Future> closeConnectionFutures = allOpenConnections.stream()
                .map(this::closeConnection)
                .collect(Collectors.toList());

        CompositeFuture.join(closeConnectionFutures).setHandler(future -> {
            if (closeHandler != null) {
                if (future.failed()) {
                    closeHandler.handle(Future.failedFuture(future.cause()));
                } else {
                    closeHandler.handle(Future.succeededFuture());
                }
            } else {
                // else not necessary
            }
        });
    }

    private Future<Void> closeConnection(CassandraConnection connection) {
        Future<Void> future = Future.future();

        connection.close(resultFuture -> {
            if (resultFuture.failed()) {
                future.fail(resultFuture.cause());
            } else {
                future.complete();
            }
        });

        return future;
    }

    @Override
    public void createConnection(ClientStateWrapper clientStateWrapper, Handler<AsyncResult<SQLConnection>> handler) {

        int connectionId = connectionIdGenerator.getAndIncrement();
        // create a new connection
        CassandraConnection connection = CassandraConnectionImpl.of(connectionId, context, workerExecutor,
                allOpenConnections, session, preparedStatementsCache);
        // add it to list instance ongoing connections
        allOpenConnections.add(connection);

        Future<SQLConnection> result = Future.succeededFuture(connection);
        handler.handle(result);
    }
}
