package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author uday
 */
final class ConnectionInfo {

    static final int DEFAULT_QUERY_TIME_OUT = 10000;

    private static final SQLOptions DEFAULT_SQL_OPTIONS = new SQLOptions()
            .setQueryTimeout(DEFAULT_QUERY_TIME_OUT);

    private final AtomicBoolean isClosed;
    private final AtomicInteger rowStreamId;
    private final Map<Integer, SQLRowStream> allRowStreams;

    private SQLOptions sqlOptions = DEFAULT_SQL_OPTIONS;

    private Context context;
    private Session session;
    private Integer connectionId;
    private WorkerExecutor workerExecutor;
    private Handler<AsyncResult<Void>> closeHandler;
    private Set<CassandraConnection> allOpenConnections;
    private Map<String, PreparedStatement> preparedStatementCache;

    private ConnectionInfo() {
        this.allRowStreams = new ConcurrentHashMap<>();
        this.rowStreamId = new AtomicInteger(1);
        this.isClosed = new AtomicBoolean(false);
    }

    static ConnectionInfo.Builder builder() {
        return new ConnectionInfo.Builder();
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

    Map<Integer, SQLRowStream> getAllRowStreams() {
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

    Optional<Handler<AsyncResult<Void>>> getCloseHandler() {
        return Optional.of(closeHandler);
    }

    Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    Integer getConnectionId() {
        return connectionId;
    }

    boolean isConnected() {
        return !isClosed.get();
    }

    void closeConnection() {
        isClosed.set(true);
    }

    public static final class Builder {

        private Context context;
        private Session session;
        private Integer connectionId;
        private WorkerExecutor workerExecutor;
        private Set<CassandraConnection> allOpenConnections;
        private Map<String, PreparedStatement> preparedStatementCache;

        private Builder() {
        }

        public Builder withConnectionId(int connectionId) {
            this.connectionId = connectionId;
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

        public ConnectionInfo build() {
            ConnectionInfo connectionInfo = new ConnectionInfo();
            connectionInfo.setContext(context)
                    .setSession(session)
                    .setConnectionId(connectionId)
                    .setWorkerExecutor(workerExecutor)
                    .setAllOpenConnections(allOpenConnections)
                    .setPreparedStatementCache(preparedStatementCache);
            return connectionInfo;
        }
    }

    void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        Objects.requireNonNull(closeHandler);
        this.closeHandler = Objects.requireNonNull(closeHandler);
    }

    void setSqlOptions(SQLOptions sqlOptions) {
        Objects.requireNonNull(sqlOptions);
        this.sqlOptions = sqlOptions;
    }

    private ConnectionInfo setConnectionId(Integer connectionId) {
        Objects.requireNonNull(connectionId);
        this.connectionId = connectionId;
        return this;
    }

    private ConnectionInfo setContext(Context context) {
        Objects.requireNonNull(context);
        this.context = context;
        return this;
    }

    private ConnectionInfo setSession(Session session) {
        Objects.requireNonNull(session);
        this.session = session;
        return this;
    }

    private ConnectionInfo setWorkerExecutor(WorkerExecutor workerExecutor) {
        Objects.requireNonNull(workerExecutor);
        this.workerExecutor = workerExecutor;
        return this;
    }

    private ConnectionInfo setPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
        Objects.requireNonNull(preparedStatementCache);
        this.preparedStatementCache = preparedStatementCache;
        return this;
    }

    private ConnectionInfo setAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
        Objects.requireNonNull(allOpenConnections);
        this.allOpenConnections = allOpenConnections;
        return this;
    }
}
