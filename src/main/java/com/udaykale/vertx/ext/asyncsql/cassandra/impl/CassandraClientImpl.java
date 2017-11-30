package com.udaykale.vertx.ext.asyncsql.cassandra.impl;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class CassandraClientImpl implements CassandraClient {
    private final Session session;
    private final Context context;
    private final AtomicBoolean isClosed;
    private final AtomicBoolean isClosing;
    private final WorkerExecutor workerExecutor;
    private final CassandraClientResources cassandraClientResources;
    private final Map<String, PreparedStatement> preparedStatementCache;

    private Handler<AsyncResult<Void>> closeHandler;
    private final AtomicInteger atomicInteger;

    CassandraClientImpl(Context context, Session session, WorkerExecutor workerExecutor) {
        this.context = Objects.requireNonNull(context);
        this.session = Objects.requireNonNull(session);
        this.workerExecutor = Objects.requireNonNull(workerExecutor);

        this.isClosing = new AtomicBoolean(false);
        this.isClosed = new AtomicBoolean(false);
        this.preparedStatementCache = new ConcurrentHashMap<>();
        this.cassandraClientResources = new CassandraClientResources();
        this.atomicInteger = new AtomicInteger(1);
    }

    @Override
    public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        Future<SQLConnection> result = Future.future();

        synchronized (this) {
            // if connection is not setClosing
            if (!isClosing.get()) {
                // create a new connection
                int connectionId = atomicInteger.getAndAdd(1);
                SQLConnection connection = new CassandraConnectionImpl(connectionId,
                        context, session, workerExecutor, preparedStatementCache);
                // add it to list instance ongoing connections
                cassandraClientResources.addConnection(connection);
                result.complete(connection);
            } else {
                // connection is doClose/setClosing
                result.fail(new IllegalStateException("Cannot create a connection when client is doClose/setClosing"));
            }
        }
        context.runOnContext(action -> result.setHandler(handler));
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = closeHandler;
        close();
    }

    @Override
    public void close() {
        isClosing.set(true);
        workerExecutor.executeBlocking(blockingFuture -> {
            synchronized (this) {
                cassandraClientResources.closeClient();
            }
            // TODO: wait till all children resources are doClose
        }, blockingFuture -> {
            isClosed.set(true);
            Future<Void> result = Future.future();
            result.complete();
            context.runOnContext(action -> result.setHandler(closeHandler));
        });
    }
}
