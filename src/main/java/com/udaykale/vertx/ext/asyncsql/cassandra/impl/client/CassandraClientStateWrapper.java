package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author uday
 */
final class CassandraClientStateWrapper {

    private Session session;
    private Context context;
    private WorkerExecutor workerExecutor;
    private Map<String, PreparedStatement> preparedStatementCache;

    private Handler<AsyncResult<Void>> closeHandler;
    private AtomicInteger connectionIdGenerator;
    private CassandraClientState currentCassandraClientState;
    private Set<CassandraConnection> allOpenConnections;

    private CassandraClientStateWrapper() {
    }

    static Builder builder() {
        return new Builder();
    }

    public Session getSession() {
        return session;
    }

    public Context getContext() {
        return context;
    }

    public WorkerExecutor getWorkerExecutor() {
        return workerExecutor;
    }

    public Map<String, PreparedStatement> getPreparedStatementCache() {
        return preparedStatementCache;
    }

    public Handler<AsyncResult<Void>> getCloseHandler() {
        return closeHandler;
    }

    public int generateConnectionId() {
        return connectionIdGenerator.getAndIncrement();
    }

    public CassandraClientState getCurrentCassandraClientState() {
        return currentCassandraClientState;
    }

    public void addConnection(CassandraConnection connection) {
        Objects.requireNonNull(connection);
        allOpenConnections.add(connection);
    }

    public void setState(CassandraClientState currentCassandraClientState) {
        this.currentCassandraClientState = Objects.requireNonNull(currentCassandraClientState);
    }

    public Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    public void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
    }

    static final class Builder {
        private Session session;
        private Context context;
        private WorkerExecutor workerExecutor;
        private CassandraClientState currentCassandraClientState;
        private Map<String, PreparedStatement> preparedStatementCache;

        private Handler<AsyncResult<Void>> closeHandler;
        private AtomicInteger connectionIdGenerator;
        private Set<CassandraConnection> allOpenConnections;

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

        public Builder withCurrentCassandraClientState(CassandraClientState currentCassandraClientState) {
            this.currentCassandraClientState = Objects.requireNonNull(currentCassandraClientState);
            return this;
        }

        public Builder withPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
            this.preparedStatementCache = Objects.requireNonNull(preparedStatementCache);
            return this;
        }

        public Builder withCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
            this.closeHandler = Objects.requireNonNull(closeHandler);
            return this;
        }

        public Builder withConnectionIdGenerator(AtomicInteger connectionIdGenerator) {
            this.connectionIdGenerator = Objects.requireNonNull(connectionIdGenerator);
            return this;
        }

        public Builder withAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
            this.allOpenConnections = Objects.requireNonNull(allOpenConnections);
            return this;
        }

        public CassandraClientStateWrapper build() {
            CassandraClientStateWrapper cassandraClientStateWrapper = new CassandraClientStateWrapper();
            cassandraClientStateWrapper.session = session;
            cassandraClientStateWrapper.context = context;
            cassandraClientStateWrapper.workerExecutor = workerExecutor;
            cassandraClientStateWrapper.currentCassandraClientState = currentCassandraClientState;
            cassandraClientStateWrapper.preparedStatementCache = preparedStatementCache;
            cassandraClientStateWrapper.closeHandler = closeHandler;
            cassandraClientStateWrapper.connectionIdGenerator = connectionIdGenerator;
            cassandraClientStateWrapper.allOpenConnections = allOpenConnections;
            return cassandraClientStateWrapper;
        }
    }
}
