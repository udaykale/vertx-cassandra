package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.TransactionIsolation;
import io.vertx.ext.sql.UpdateResult;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionUtil.handleUpdate;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryAndRun;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParamsAndRun;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParamsRowMapper;
import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

public final class CassandraConnectionImpl implements CassandraConnection {

    private final Context context;
    private final int connectionId;
    private final AtomicBoolean lock;
    private final ConnectionStateWrapper connectionStateWrapper;

    private static final int DEFAULT_QUERY_TIME_OUT = 10000;
    private static final SQLOptions DEFAULT_SQL_OPTIONS = new SQLOptions()
            .setQueryTimeout(DEFAULT_QUERY_TIME_OUT);
    private SQLOptions sqlOptions = DEFAULT_SQL_OPTIONS;

    private Handler<AsyncResult<Void>> closeHandler;

    private CassandraConnectionImpl(int connectionId, ConnectionStateWrapper connectionStateWrapper,
                                    Context context, AtomicBoolean lock) {
        this.connectionId = connectionId;
        this.lock = Objects.requireNonNull(lock);
        this.context = Objects.requireNonNull(context);
        this.connectionStateWrapper = Objects.requireNonNull(connectionStateWrapper);
    }

    public static CassandraConnectionImpl of(Integer connectionId, Context context, WorkerExecutor workerExecutor,
                                             Set<CassandraConnection> allOpenConnections, Session session,
                                             Map<String, PreparedStatement> preparedStatementCache) {

        CassandraConnectionState currentState = ConnectionStreamState.instance(context, workerExecutor, new HashSet<>(),
                session, preparedStatementCache, new AtomicInteger(1), allOpenConnections);
        AtomicBoolean lock = new AtomicBoolean();
        ConnectionStateWrapper connectionStateWrapper = ConnectionStateWrapper.of(currentState);
        return new CassandraConnectionImpl(connectionId, connectionStateWrapper, context, lock);
    }

    @Override
    public SQLConnection execute(String query, Handler<AsyncResult<Void>> handler) {
        validateQueryAndRun(query, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query, null, null,
                        sqlOptions, future -> {
                            if (future.succeeded()) {
                                handler.handle(Future.succeededFuture());
                            } else {
                                handler.handle(Future.failedFuture(future.cause()));
                            }
                        }));
        return this;
    }

    @Override
    public SQLConnection query(String query, Handler<AsyncResult<ResultSet>> handler) {
        validateQueryAndRun(query, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query,
                        null, null, sqlOptions, handler));
        return this;
    }

    @Override
    public SQLConnection queryWithParams(String query, JsonArray params, Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParamsAndRun(query, params, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query, params,
                        null, sqlOptions, handler));
        return this;
    }

    @Override
    public CassandraConnection queryWithParams(String query, JsonArray params,
                                               Function<Row, JsonArray> rowMapper,
                                               Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query, params,
                        rowMapper, sqlOptions, handler));
        return this;
    }

    @Override
    public SQLConnection queryStream(String query, Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryAndRun(query, context, handler, nextOperation ->
                CassandraConnectionStreamHelper.of(lock).queryStreamWithParams(connectionStateWrapper,
                        singletonList(query), emptyList(), sqlOptions, null, handler));
        return this;
    }

    @Override
    public SQLConnection queryStreamWithParams(String query, JsonArray params,
                                               Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParamsAndRun(query, params, context, handler, nextOperation ->
                CassandraConnectionStreamHelper.of(lock).queryStreamWithParams(connectionStateWrapper,
                        singletonList(query), singletonList(params), sqlOptions, null, handler));
        return this;
    }

    @Override
    public CassandraConnection queryStreamWithParams(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                                     Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, context, handler, nextOperation ->
                CassandraConnectionStreamHelper.of(lock).queryStreamWithParams(connectionStateWrapper,
                        singletonList(query), singletonList(params), sqlOptions, rowMapper, handler));
        return this;
    }

    @Override
    public SQLConnection update(String query, Handler<AsyncResult<UpdateResult>> handler) {
        validateQueryAndRun(query, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query,
                        null, null, sqlOptions,
                        future -> handleUpdate(handler, future)));
        return this;
    }


    @Override
    public SQLConnection updateWithParams(String query, JsonArray params,
                                          Handler<AsyncResult<UpdateResult>> handler) {
        validateQueryParamsAndRun(query, params, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, query, params,
                        null, sqlOptions,
                        future -> handleUpdate(handler, future)));
        return this;
    }

    @Override
    public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
        validateBatch(sqlStatements, context, handler, nextOperation ->
                CassandraConnectionUtil.queryWithParams(lock, connectionStateWrapper, sqlStatements,
                        null, null, sqlOptions, future -> {
                            if (future.succeeded()) {
                                handler.handle(Future.succeededFuture(nCopies(sqlStatements.size(), -1)));
                            } else {
                                handler.handle(Future.failedFuture(future.cause()));
                            }
                        }));
        return this;
    }

    @Override
    public SQLConnection batchWithParams(String query, List<JsonArray> args,
                                         Handler<AsyncResult<List<Integer>>> handler) {
        // TODO
//        validateQueryAndRun(query, handler);
//        Objects.requireNonNull(args);
//        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, singletonList(query), args, null,
//                future -> handleBatch(args.size(), handler, context, future));
        return this;
    }

    @Override
    public CassandraConnection batchWithParams(List<String> sqlStatements, List<JsonArray> args,
                                               Handler<AsyncResult<List<Integer>>> handler) {

//        validateBatch(sqlStatements, handler);
//        Objects.requireNonNull(args);
//        assert sqlStatements.size() == args.size();
//        CassandraConnectionUtil.queryWithParams(connectionInfoWrapper, sqlStatements, args, null,
//                future -> handleBatch(args.size(), handler, context, future));
        return this;
    }

    @Override
    public int connectionId() {
        return connectionId;
    }

    @Override
    public SQLConnection call(String sql, Handler<AsyncResult<ResultSet>> handler) {
        // TODO
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs,
                                        Handler<AsyncResult<ResultSet>> handler) {
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
        close(v -> Future.succeededFuture());
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
        synchronized (lock) {
            context.runOnContext(v -> connectionStateWrapper.close(this, closeHandler));
        }
    }

    @Override
    public SQLConnection setOptions(SQLOptions options) {
        if (options == null) {
            synchronized (lock) {
                Throwable exception = new NullPointerException("SQL options cannot be null");
                connectionStateWrapper.setState(ConnectionParameterErrorState.instance(exception));
            }
        } else {
            SQLOptions sqlOptions = new SQLOptions(options);

            if (sqlOptions.getQueryTimeout() < 1) {
                sqlOptions.setQueryTimeout(DEFAULT_QUERY_TIME_OUT);
            }

            this.sqlOptions = sqlOptions;
        }

        return this;
    }

    @Override
    public SQLConnection setAutoCommit(boolean autoCommit, Handler<AsyncResult<Void>> handler) {
        return unsupported(handler, "Cassandra does not support auto commit flag.");
    }

    @Override
    public SQLConnection commit(Handler<AsyncResult<Void>> handler) {
        return unsupported(handler, "Cassandra does not support commit.");
    }

    @Override
    public SQLConnection rollback(Handler<AsyncResult<Void>> handler) {
        return unsupported(handler, "Cassandra does not support rollback.");
    }

    @Override
    public SQLConnection setTransactionIsolation(TransactionIsolation isolation, Handler<AsyncResult<Void>> handler) {
        return unsupported(handler, "Cassandra does not support Transaction Isolation.");
    }

    @Override
    public SQLConnection getTransactionIsolation(Handler<AsyncResult<TransactionIsolation>> handler) {
        return unsupported(handler, "Cassandra does not support Transaction Isolation.");
    }

    private <T> SQLConnection unsupported(Handler<AsyncResult<T>> handler, String message) {
        Throwable t = new UnsupportedOperationException(message);
        handler.handle(Future.failedFuture(t));
        return this;
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
