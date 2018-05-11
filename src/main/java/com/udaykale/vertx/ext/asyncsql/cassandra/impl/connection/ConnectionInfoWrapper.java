package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author uday
 */
final class ConnectionInfoWrapper {

    static final int DEFAULT_QUERY_TIME_OUT = 10000;

    private static final SQLOptions DEFAULT_SQL_OPTIONS = new SQLOptions()
            .setQueryTimeout(DEFAULT_QUERY_TIME_OUT);

    private final AtomicInteger rowStreamId;
    private final Set<SQLRowStream> allRowStreams;

    private SQLOptions sqlOptions = DEFAULT_SQL_OPTIONS;

    private Context context;
    private Session session;
    private AtomicBoolean lock;
    private WorkerExecutor workerExecutor;
    private CassandraConnectionState state;
    private Set<CassandraConnection> allOpenConnections;
    private Map<String, PreparedStatement> preparedStatementCache;

    private ConnectionInfoWrapper() {
        this.allRowStreams = new ConcurrentHashSet<>();
        this.rowStreamId = new AtomicInteger(1);
    }

    static ConnectionInfoWrapper.Builder builder() {
        return new ConnectionInfoWrapper.Builder();
    }

    Context getContext() {
        return context;
    }

    Session getSession() {
        return session;
    }

    WorkerExecutor getWorkerExecutor() {
        return workerExecutor;
    }

    Set<SQLRowStream> getAllRowStreams() {
        return allRowStreams;
    }

    Map<String, PreparedStatement> getPreparedStatementCache() {
        return preparedStatementCache;
    }

    int generateStreamId() {
        return rowStreamId.getAndIncrement();
    }

    SQLOptions getSqlOptions() {
        return sqlOptions;
    }

    Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    AtomicBoolean getLock() {
        return lock;
    }

    void close(CassandraConnection connection, Handler<AsyncResult<Void>> closeHandler) {
        state.close(this, connection, closeHandler);
    }

    void stream(List<String> queries, List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                Handler<AsyncResult<SQLRowStream>> handler) {
        state.stream(this, queries, params, rowMapper, handler);
    }

    ConnectionInfoWrapper setState(CassandraConnectionState state) {
        Objects.requireNonNull(state);
        this.state = state;
        return this;
    }

    public static final class Builder {

        private Context context;
        private Session session;
        private AtomicBoolean lock;
        private WorkerExecutor workerExecutor;
        private CassandraConnectionState currentState;
        private Set<CassandraConnection> allOpenConnections;
        private Map<String, PreparedStatement> preparedStatementCache;

        private Builder() {
        }

        public Builder withLock(AtomicBoolean lock) {
            this.lock = lock;
            return this;
        }

        public Builder withContext(Context context) {
            this.context = context;
            return this;
        }

        public Builder withSession(Session session) {
            this.session = session;
            return this;
        }

        public Builder withWorkerExecutor(WorkerExecutor workerExecutor) {
            this.workerExecutor = workerExecutor;
            return this;
        }

        public Builder withPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
            this.preparedStatementCache = preparedStatementCache;
            return this;
        }

        public Builder withAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
            this.allOpenConnections = allOpenConnections;
            return this;
        }

        public Builder withState(CassandraConnectionState currentState) {
            this.currentState = currentState;
            return this;
        }

        public ConnectionInfoWrapper build() {
            ConnectionInfoWrapper connectionInfoWrapper = new ConnectionInfoWrapper();
            connectionInfoWrapper.setLock(lock)
                    .setContext(context)
                    .setSession(session)
                    .setState(currentState)
                    .setWorkerExecutor(workerExecutor)
                    .setAllOpenConnections(allOpenConnections)
                    .setPreparedStatementCache(preparedStatementCache);
            return connectionInfoWrapper;
        }
    }

    void setSqlOptions(SQLOptions sqlOptions) {
        Objects.requireNonNull(sqlOptions);
        this.sqlOptions = sqlOptions;
    }

    private ConnectionInfoWrapper setLock(AtomicBoolean lock) {
        Objects.requireNonNull(lock);
        this.lock = lock;
        return this;
    }

    private ConnectionInfoWrapper setContext(Context context) {
        Objects.requireNonNull(context);
        this.context = context;
        return this;
    }

    private ConnectionInfoWrapper setSession(Session session) {
        Objects.requireNonNull(session);
        this.session = session;
        return this;
    }

    private ConnectionInfoWrapper setWorkerExecutor(WorkerExecutor workerExecutor) {
        Objects.requireNonNull(workerExecutor);
        this.workerExecutor = workerExecutor;
        return this;
    }

    private ConnectionInfoWrapper setAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
        Objects.requireNonNull(allOpenConnections);
        this.allOpenConnections = allOpenConnections;
        return this;
    }

    private void setPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
        Objects.requireNonNull(preparedStatementCache);
        this.preparedStatementCache = preparedStatementCache;
    }
}
