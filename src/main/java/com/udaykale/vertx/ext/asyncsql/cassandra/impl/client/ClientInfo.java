package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author uday
 */
final class ClientInfo {

    private CassandraClientState state;

    private final Session session;
    private final Context context;
    private final WorkerExecutor workerExecutor;
    private final AtomicInteger connectionIdGenerator;
    private final Set<CassandraConnection> allOpenConnections;
    private final Map<String, PreparedStatement> preparedStatementCache;

    private ClientInfo(Context context, Session session, WorkerExecutor workerExecutor, CassandraClientState state) {
        this.state = Objects.requireNonNull(state);
        this.session = Objects.requireNonNull(session);
        this.context = Objects.requireNonNull(context);
        this.workerExecutor = Objects.requireNonNull(workerExecutor);
        this.allOpenConnections = new ConcurrentHashSet<>();
        this.preparedStatementCache = new ConcurrentHashMap<>();
        this.connectionIdGenerator = new AtomicInteger(1);
    }

    static ClientInfo of(Context context, Session session, WorkerExecutor workerExecutor, CassandraClientState state) {
        return new ClientInfo(context, session, workerExecutor, state);
    }

    Session getSession() {
        return session;
    }

    Context getContext() {
        return context;
    }

    WorkerExecutor getWorkerExecutor() {
        return workerExecutor;
    }

    Map<String, PreparedStatement> getPreparedStatementCache() {
        return preparedStatementCache;
    }

    int generateConnectionId() {
        return connectionIdGenerator.getAndIncrement();
    }

    public Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    CassandraClientState getState() {
        return state;
    }

    void addConnection(CassandraConnection connection) {
        Objects.requireNonNull(connection);
        allOpenConnections.add(connection);
    }

    public void setState(CassandraClientState clientState) {
        this.state = Objects.requireNonNull(clientState);
    }
}
