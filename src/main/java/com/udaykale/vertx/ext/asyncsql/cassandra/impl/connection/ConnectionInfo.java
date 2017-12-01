package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author uday
 */
final class ConnectionInfo {

    private Context context;
    private Session session;
    private WorkerExecutor workerExecutor;
    private Set<SQLRowStream> allRowStreams;
    private Map<String, PreparedStatement> preparedStatementCache;
    private AtomicInteger rowStreamId;

    private SQLOptions sqlOptions;
    private Handler<AsyncResult<Void>> closeHandler;

    private ConnectionInfo() {
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

    public static final class Builder {

        private Context context;
        private Session session;
        private WorkerExecutor workerExecutor;
        private Set<SQLRowStream> allRowStreams;
        private Map<String, PreparedStatement> preparedStatementCache;
        private AtomicInteger rowStreamId;

        private SQLOptions sqlOptions;
        private Handler<AsyncResult<Void>> closeHandler;

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

        public Builder withAllRowStreams(Set<SQLRowStream> allRowStreams) {
            this.allRowStreams = Objects.requireNonNull(allRowStreams);
            return this;
        }

        public Builder withPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
            this.preparedStatementCache = Objects.requireNonNull(preparedStatementCache);
            return this;
        }

        public Builder withRowStreamId(AtomicInteger rowStreamId) {
            this.rowStreamId = Objects.requireNonNull(rowStreamId);
            return this;
        }

        public Builder withSqlOptions(SQLOptions sqlOptions) {
            this.sqlOptions = sqlOptions;
            return this;
        }

        public Builder withCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
            this.closeHandler = closeHandler;
            return this;
        }

        public ConnectionInfo build() {
            ConnectionInfo connectionInfo = new ConnectionInfo();
            connectionInfo.setContext(context);
            connectionInfo.setSession(session);
            connectionInfo.setWorkerExecutor(workerExecutor);
            connectionInfo.setAllRowStreams(allRowStreams);
            connectionInfo.setPreparedStatementCache(preparedStatementCache);
            connectionInfo.setRowStreamId(rowStreamId);
            connectionInfo.setSqlOptions(sqlOptions);
            connectionInfo.setCloseHandler(closeHandler);
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

    private void setAllRowStreams(Set<SQLRowStream> allRowStreams) {
        this.allRowStreams = allRowStreams;
    }

    private void setPreparedStatementCache(Map<String, PreparedStatement> preparedStatementCache) {
        this.preparedStatementCache = preparedStatementCache;
    }

    private void setRowStreamId(AtomicInteger rowStreamId) {
        this.rowStreamId = rowStreamId;
    }

    public void setSqlOptions(SQLOptions sqlOptions) {
        this.sqlOptions = sqlOptions;
    }

    public void setCloseHandler(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = closeHandler;
    }
}
