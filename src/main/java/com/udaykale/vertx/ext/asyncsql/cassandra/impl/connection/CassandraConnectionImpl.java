package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.TransactionIsolation;
import io.vertx.ext.sql.UpdateResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionUtil.handleBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionUtil.handleQuery;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionUtil.handleUpdate;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQuery;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParams;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParamsRowMapper;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.ConnectionInfoWrapper.DEFAULT_QUERY_TIME_OUT;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * @author uday
 */
public final class CassandraConnectionImpl implements CassandraConnection {

    private final Context context;
    private final int connectionId;
    private final AtomicBoolean lock;
    private final ConnectionInfoWrapper connectionInfoWrapper;

    private Handler<AsyncResult<Void>> closeHandler;

    private CassandraConnectionImpl(int connectionId, ConnectionInfoWrapper connectionInfoWrapper,
                                    Context context, AtomicBoolean lock) {
        this.connectionId = connectionId;
        this.lock = Objects.requireNonNull(lock);
        this.context = Objects.requireNonNull(context);
        this.connectionInfoWrapper = Objects.requireNonNull(connectionInfoWrapper);
    }

    public static CassandraConnectionImpl of(Integer connectionId, Context context,
                                             Set<CassandraConnection> allOpenConnections,
                                             Session session, WorkerExecutor workerExecutor,
                                             Map<String, PreparedStatement> preparedStatementCache) {
        CassandraConnectionState currentState = ConnectionStreamState.instance();
        AtomicBoolean lock = new AtomicBoolean();
        ConnectionInfoWrapper connectionInfoWrapper = ConnectionInfoWrapper.builder()
                .withLock(lock)
                .withContext(context)
                .withSession(session)
                .withState(currentState)
                .withWorkerExecutor(workerExecutor)
                .withAllOpenConnections(allOpenConnections)
                .withPreparedStatementCache(preparedStatementCache)
                .build();
        return new CassandraConnectionImpl(connectionId, connectionInfoWrapper, context, lock);
    }

    @Override
    public SQLConnection execute(String query, Handler<AsyncResult<Void>> handler) {
        validateQuery(query, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, null, null,
                future -> handleQuery(handler, context, future));
        return this;
    }

    @Override
    public SQLConnection query(String query, Handler<AsyncResult<ResultSet>> handler) {
        validateQuery(query, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, null, null, handler);
        return this;
    }

    @Override
    public SQLConnection queryWithParams(String query, JsonArray params,
                                         Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, params, null, handler);
        return this;
    }

    @Override
    public CassandraConnection queryWithParams(String query, JsonArray params,
                                               Function<Row, JsonArray> rowMapper,
                                               Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, params, rowMapper, handler);
        return this;
    }

    @Override
    public SQLConnection queryStream(String query, Handler<AsyncResult<SQLRowStream>> handler) {
        validateQuery(query, handler);
        CassandraConnectionUtil.queryStreamWithParams(connectionInfoWrapper, singletonList(query),
                emptyList(), null, handler);
        return this;
    }

    @Override
    public SQLConnection queryStreamWithParams(String query, JsonArray params,
                                               Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionUtil.queryStreamWithParams(connectionInfoWrapper, singletonList(query),
                singletonList(params), null, handler);
        return this;
    }

    @Override
    public CassandraConnection queryStreamWithParams(String query, JsonArray params,
                                                     Function<Row, JsonArray> rowMapper,
                                                     Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, handler);
        CassandraConnectionUtil.queryStreamWithParams(connectionInfoWrapper, singletonList(query),
                singletonList(params), rowMapper, handler);
        return this;
    }

    @Override
    public SQLConnection update(String query, Handler<AsyncResult<UpdateResult>> handler) {
        validateQuery(query, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, null, null,
                future -> handleUpdate(handler, context, future));
        return this;
    }

    @Override
    public SQLConnection updateWithParams(String query, JsonArray params,
                                          Handler<AsyncResult<UpdateResult>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, query, params, null,
                future -> handleUpdate(handler, context, future));
        return this;
    }

    @Override
    public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
        validateBatch(sqlStatements, handler);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, sqlStatements, null, null,
                future -> handleBatch(sqlStatements.size(), handler, context, future));
        return this;
    }

    @Override
    public SQLConnection batchWithParams(String query, List<JsonArray> args,
                                         Handler<AsyncResult<List<Integer>>> handler) {
        // TODO
        validateQuery(query, handler);
        Objects.requireNonNull(args);
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, singletonList(query), args, null,
                future -> handleBatch(args.size(), handler, context, future));
        return this;
    }

    @Override
    public CassandraConnection batchWithParams(List<String> sqlStatements, List<JsonArray> args,
                                               Handler<AsyncResult<List<Integer>>> handler) {

        validateBatch(sqlStatements, handler);
        Objects.requireNonNull(args);
        assert sqlStatements.size() == args.size();
        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, sqlStatements, args, null,
                future -> handleBatch(args.size(), handler, context, future));
        return this;
    }

    @Override
    public int connectionId() {
        return connectionId;
    }

    @Override
    public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
        // TODO
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs,
                                        Handler<AsyncResult<ResultSet>> resultHandler) {
        // TODO
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public SQLConnection batchCallableWithParams(String sqlStatement, List<JsonArray> inArgs, List<JsonArray> outArgs,
                                                 Handler<AsyncResult<List<Integer>>> handler) {
        // TODO
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public void close() {
        synchronized (lock) {
            connectionInfoWrapper.close(this, closeHandler);
        }
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
        close();
    }

    @Override
    public SQLConnection setOptions(SQLOptions options) {
        SQLOptions sqlOptions;

        if (options == null) {
            synchronized (lock) {
                connectionInfoWrapper.setState(ConnectionParameterErrorState.instance());
            }
            throw new NullPointerException("SQL options cannot be null");
        } else {
            sqlOptions = options;
        }

        if (sqlOptions.getQueryTimeout() < 1) {
            sqlOptions.setQueryTimeout(DEFAULT_QUERY_TIME_OUT);
        }

        connectionInfoWrapper.setSqlOptions(sqlOptions);

        return this;
    }

    @Override
    public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> resultHandler) {
        throw new UnsupportedOperationException("Cassandra does not support auto commit flag.");
    }

    @Override
    public SQLConnection commit(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException("Cassandra does not support commit.");
    }

    @Override
    public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException("Cassandra does not support rollback.");
    }

    @Override
    public SQLConnection setTransactionIsolation(TransactionIsolation isolation,
                                                 Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException("Cassandra does not support Transaction Isolation.");
    }

    @Override
    public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
        throw new UnsupportedOperationException("Cassandra does not support Transaction Isolation.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraConnectionImpl that = (CassandraConnectionImpl) o;
        return this.connectionId == that.connectionId;
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "CassandraConnectionImpl{" +
                "connectionId=" + connectionId +
                '}';
    }
}
