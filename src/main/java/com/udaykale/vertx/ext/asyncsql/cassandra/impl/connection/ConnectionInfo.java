package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author uday
 */
final class ConnectionInfo {

    static final int DEFAULT_QUERY_TIME_OUT = 10000;

    private static final SQLOptions DEFAULT_SQL_OPTIONS = new SQLOptions()
            .setQueryTimeout(DEFAULT_QUERY_TIME_OUT);

    private final CassandraConnection cassandraConnection;
    private final Set<SQLRowStream> allRowStreams = new ConcurrentHashSet<>();
    private final AtomicInteger rowStreamId = new AtomicInteger(1);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private SQLOptions sqlOptions = DEFAULT_SQL_OPTIONS;

    private Context context;
    private Session session;
    private WorkerExecutor workerExecutor;
    private Handler<AsyncResult<Void>> closeHandler;
    private Set<CassandraConnection> allOpenConnections;
    private Map<String, PreparedStatement> preparedStatementCache;

    private ConnectionInfo(CassandraConnection cassandraConnection) {
        this.cassandraConnection = cassandraConnection;
    }

    static ConnectionInfo.Builder builder(CassandraConnection cassandraConnection) {
        return new ConnectionInfo.Builder(cassandraConnection);
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

    AtomicInteger getRowStreamId() {
        return rowStreamId;
    }

    SQLOptions getSqlOptions() {
        return sqlOptions;
    }

    Handler<AsyncResult<Void>> getCloseHandler() {
        return closeHandler;
    }

    Set<CassandraConnection> getAllOpenConnections() {
        return allOpenConnections;
    }

    CassandraConnection getConnection() {
        return cassandraConnection;
    }

    boolean isConnected() {
        return !isClosed.get();
    }

    void closeConnection() {
        isClosed.set(true);
    }

    void setSqlOptions(SQLOptions sqlOptions) {
        this.sqlOptions = sqlOptions;
    }

    public static final class Builder {
        private Context context;
        private Session session;
        private WorkerExecutor workerExecutor;
        private Set<CassandraConnection> allOpenConnections;
        private final CassandraConnection cassandraConnection;
        private Map<String, PreparedStatement> preparedStatementCache;

        Builder(CassandraConnection cassandraConnection) {
            this.cassandraConnection = Objects.requireNonNull(cassandraConnection);
        }

        public Builder withContext(Context context) {
            this.context = Objects.requireNonNull(context);
            return this;
        }

        public Builder withSession(Session session) {
            this.session = Objects.requireNonNull(session);
            return this;
        }

        public Builder withWorkerExecutor(WorkerExecutor workerExecutor) {
            this.workerExecutor = Objects.requireNonNull(workerExecutor);
            return this;
        }

        public Builder withPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
            this.preparedStatementCache = Objects.requireNonNull(preparedStatementCache);
            return this;
        }

        public Builder withAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
            this.allOpenConnections = Objects.requireNonNull(allOpenConnections);
            return this;
        }

        public ConnectionInfo build() {
            ConnectionInfo connectionInfo = new ConnectionInfo(cassandraConnection);
            connectionInfo.setContext(context);
            connectionInfo.setSession(session);
            connectionInfo.setWorkerExecutor(workerExecutor);
            connectionInfo.setAllOpenConnections(allOpenConnections);
            connectionInfo.setPreparedStatementCache(preparedStatementCache);
            return connectionInfo;
        }
    }

    private void setContext(Context context) {
        this.context = context;
    }

    private void setSession(Session session) {
        this.session = session;
    }

    private void setWorkerExecutor(WorkerExecutor workerExecutor) {
        this.workerExecutor = workerExecutor;
    }

    private void setPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
        this.preparedStatementCache = preparedStatementCache;
    }

    private void setAllOpenConnections(Set<CassandraConnection> allOpenConnections) {
        this.allOpenConnections = allOpenConnections;
    }

    void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
    }
}
