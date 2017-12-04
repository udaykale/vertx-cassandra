package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
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

    private Session session;
    private Context context;
    private WorkerExecutor workerExecutor;
    private Handler<AsyncResult<Void>> closeHandler;
    private CassandraClientState clientState;

    private final AtomicInteger connectionIdGenerator;
    private final Set<CassandraConnection> allOpenConnections;
    private final Map<String, PreparedStatement> preparedStatementCache;

    private ClientInfo(CassandraClient cassandraClient) {
        this.allOpenConnections = new ConcurrentHashSet<>();
        this.preparedStatementCache = new ConcurrentHashMap<>();
        this.connectionIdGenerator = new AtomicInteger(1);
        this.clientState = CreatingConnectionClientState.instance(cassandraClient);
    }

    static Builder builder(CassandraClient cassandraClient) {
        return new Builder(cassandraClient);
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

    Handler<AsyncResult<Void>> getCloseHandler() {
        return closeHandler;
    }

    int generateConnectionId() {
        return connectionIdGenerator.getAndIncrement();
    }

    CassandraClientState getCurrentCassandraClientState() {
        return clientState;
    }

    void addConnection(CassandraConnection connection) {
        Objects.requireNonNull(connection);
        allOpenConnections.add(connection);
    }

    void closeAllOpenConnections() {
        for (CassandraConnection cassandraConnection : allOpenConnections) {
            cassandraConnection.close();
        }
    }

    void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
    }

    public void setState(CassandraClientState clientState) {
        this.clientState = Objects.requireNonNull(clientState);
    }

    public Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    static final class Builder {
        private final CassandraClient cassandraClient;

        private Session session;
        private Context context;
        private WorkerExecutor workerExecutor;

        private Builder(CassandraClient cassandraClient) {
            this.cassandraClient = Objects.requireNonNull(cassandraClient);
        }

        public Builder withSession(Session session) {
            this.session = Objects.requireNonNull(session);
            return this;
        }

        public Builder withContext(Context context) {
            this.context = Objects.requireNonNull(context);
            return this;
        }

        public Builder withWorkerExecutor(WorkerExecutor workerExecutor) {
            this.workerExecutor = Objects.requireNonNull(workerExecutor);
            return this;
        }

        public ClientInfo build() {
            ClientInfo clientInfo = new ClientInfo(cassandraClient);
            clientInfo.workerExecutor = workerExecutor;
            clientInfo.session = session;
            clientInfo.context = context;
            return clientInfo;
        }
    }
}
