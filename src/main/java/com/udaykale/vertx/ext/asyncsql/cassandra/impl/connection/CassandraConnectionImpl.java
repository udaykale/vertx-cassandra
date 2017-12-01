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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionHelper.emptyListIfNull;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionHelper.handleBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionHelper.handleQuery;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionHelper.handleUpdate;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQuery;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParams;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionValidationUtils.validateQueryParamsRowMapper;
import static java.util.Collections.singletonList;

/**
 * @author uday
 */
public final class CassandraConnectionImpl implements CassandraConnection {

    private static final int DEFAULT_QUERY_TIME_OUT = 10000;

    private final Context context;
    private final int connectionId;
    private final ConnectionInfo connectionInfo;

    public CassandraConnectionImpl(int connectionId, Context context,
                                   Session session, WorkerExecutor workerExecutor,
                                   Map<String, PreparedStatement> preparedStatementCache) {
        this.connectionId = connectionId;
        this.context = Objects.requireNonNull(context);
        this.connectionInfo = ConnectionInfo.builder()
                .withContext(context)
                .withSession(session)
                .withWorkerExecutor(workerExecutor)
                .withPreparedStatementCache(preparedStatementCache)
                .withAllRowStreams(new ConcurrentSkipListSet<>())
                .withSqlOptions(new SQLOptions().setQueryTimeout(DEFAULT_QUERY_TIME_OUT))
                .withRowStreamId(new AtomicInteger(1))
                .build();
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
    public SQLConnection execute(String query, Handler<AsyncResult<Void>> handler) {
        validateQuery(query, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, null, null,
                future -> handleQuery(handler, future, context));
        return this;
    }

    @Override
    public SQLConnection query(String query, Handler<AsyncResult<ResultSet>> handler) {
        validateQuery(query, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, null, null, handler);
        return this;
    }

    @Override
    public SQLConnection queryWithParams(String query, JsonArray params,
                                         Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, params, null, handler);
        return this;
    }

    @Override
    public CassandraConnection queryWithParams(String query, JsonArray params,
                                               Function<Row, JsonArray> rowMapper,
                                               Handler<AsyncResult<ResultSet>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, params, rowMapper, handler);
        return this;
    }

    @Override
    public SQLConnection queryStream(String query, Handler<AsyncResult<SQLRowStream>> handler) {
        validateQuery(query, handler);
        CassandraConnectionHelper.queryStreamWithParams(connectionInfo, singletonList(query),
                null, null, handler);
        return this;
    }

    @Override
    public SQLConnection queryStreamWithParams(String query, JsonArray params,
                                               Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionHelper.queryStreamWithParams(connectionInfo, singletonList(query),
                emptyListIfNull(params), null, handler);
        return this;
    }

    @Override
    public CassandraConnection queryStreamWithParams(String query, JsonArray params,
                                                     Function<Row, JsonArray> rowMapper,
                                                     Handler<AsyncResult<SQLRowStream>> handler) {
        validateQueryParamsRowMapper(query, params, rowMapper, handler);
        CassandraConnectionHelper.queryStreamWithParams(connectionInfo, singletonList(query),
                emptyListIfNull(params), rowMapper, handler);
        return this;
    }

    @Override
    public SQLConnection update(String query, Handler<AsyncResult<UpdateResult>> handler) {
        validateQuery(query, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, null, null,
                future -> handleUpdate(handler, context, future));
        return this;
    }

    @Override
    public SQLConnection updateWithParams(String query, JsonArray params,
                                          Handler<AsyncResult<UpdateResult>> handler) {
        validateQueryParams(query, params, handler);
        CassandraConnectionHelper.queryWithParams(connectionInfo, query, params, null,
                future -> handleUpdate(handler, context, future));
        return this;
    }

    @Override
    public SQLConnection batch(List<String> sqlStatements, Handler<AsyncResult<List<Integer>>> handler) {
        validateBatch(sqlStatements, handler);

        CassandraConnectionHelper.queryWithParams(connectionInfo, sqlStatements, null, null,
                future -> handleBatch(sqlStatements.size(), handler, context, future));

        return this;
    }

    @Override
    public SQLConnection batchWithParams(String query, List<JsonArray> args,
                                         Handler<AsyncResult<List<Integer>>> handler) {
        validateQuery(query, handler);
        Objects.requireNonNull(args);
        CassandraConnectionHelper.queryWithParams(connectionInfo, singletonList(query), args, null,
                future -> handleBatch(args.size(), handler, context, future));
        return this;
    }

    @Override
    public CassandraConnection batchWithParams(List<String> sqlStatements, List<JsonArray> args,
                                               Handler<AsyncResult<List<Integer>>> handler) {

        validateBatch(sqlStatements, handler);
        Objects.requireNonNull(args);
        assert sqlStatements.size() == args.size();
        CassandraConnectionHelper.queryWithParams(connectionInfo, sqlStatements, args, null,
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
        return this;
    }

    @Override
    public SQLConnection callWithParams(String sql, JsonArray params, JsonArray outputs,
                                        Handler<AsyncResult<ResultSet>> resultHandler) {
        // TODO
        return this;
    }

    @Override
    public SQLConnection batchCallableWithParams(String sqlStatement, List<JsonArray> inArgs,
                                                 List<JsonArray> outArgs,
                                                 Handler<AsyncResult<List<Integer>>> handler) {
        // TODO
        return this;
    }

    @Override
    public void close() {
        // TODO
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        connectionInfo.setCloseHandler(closeHandler);
        close();
    }

    @Override
    public SQLConnection setOptions(SQLOptions options) {
        SQLOptions sqlOptions = Objects.requireNonNull(options);

        if (sqlOptions.getQueryTimeout() < 1) {
            sqlOptions.setQueryTimeout(DEFAULT_QUERY_TIME_OUT);
        }

        connectionInfo.setSqlOptions(sqlOptions);

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
}