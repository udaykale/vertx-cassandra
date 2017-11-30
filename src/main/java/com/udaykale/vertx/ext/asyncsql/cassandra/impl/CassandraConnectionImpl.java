package com.udaykale.vertx.ext.asyncsql.cassandra.impl;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.CassandraRowStream;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.CassandraConnectionHelper.generateStatement;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.CassandraConnectionHelper.handleBatch;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.CassandraConnectionHelper.handleQuery;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.CassandraConnectionHelper.handleUpdate;
import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.CassandraConnectionHelper.resultSetOf;
import static java.util.Collections.singletonList;

final class CassandraConnectionImpl implements CassandraConnection {

    private static final int QUERY_TIME_OUT = 10000;

    private final Context context;
    private final Session session;
    private final int connectionId;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams; // There will always be one row stream per connection
    private final Map<String, PreparedStatement> preparedStatementCache;

    private SQLOptions sqlOptions;
    private Handler<AsyncResult<Void>> closeHandler;

    CassandraConnectionImpl(int connectionId, Context ctx, Session session, WorkerExecutor workerExecutor,
                            Map<String, PreparedStatement> preparedStatementCache) {
        this.connectionId = connectionId;
        this.context = Objects.requireNonNull(ctx);
        this.session = Objects.requireNonNull(session);
        this.workerExecutor = Objects.requireNonNull(workerExecutor);
        this.preparedStatementCache = Objects.requireNonNull(preparedStatementCache);

        this.allRowStreams = new HashSet<>();
        this.sqlOptions = new SQLOptions().setQueryTimeout(QUERY_TIME_OUT);
    }

    @Override
    public SQLConnection execute(String query, Handler<AsyncResult<Void>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(handler);

        return queryWithParams(query, null, null, context,
                future -> handleQuery(handler, future, context));
    }

    @Override
    public SQLConnection query(String query, Handler<AsyncResult<ResultSet>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(handler);

        return queryWithParams(query, null, null, context, handler);
    }

    @Override
    public SQLConnection queryWithParams(String query, JsonArray params,
                                         Handler<AsyncResult<ResultSet>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(params);
        Objects.requireNonNull(handler);

        return queryWithParams(query, params, null, context, handler);
    }

    @Override
    public SQLConnection queryWithParams(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                         Handler<AsyncResult<ResultSet>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(params);
        Objects.requireNonNull(handler);
        Objects.requireNonNull(rowMapper);

        return queryWithParams(query, params, rowMapper, context, handler);
    }

    private SQLConnection queryWithParams(String query, JsonArray params, Function<Row, JsonArray> rowMapper,
                                          Context context, Handler<AsyncResult<ResultSet>> resultHandler) {
        Objects.requireNonNull(query);
        List<JsonArray> jsonArrays = new LinkedList<>();
        Future<ResultSet> result = Future.future();

        queryStreamWithParams(singletonList(query),
                params == null ? Collections.EMPTY_LIST : singletonList(params),
                rowMapper, queryResult -> {
                    if (queryResult.succeeded()) {
                        SQLRowStream sqlRowStream = queryResult.result();

                        sqlRowStream.resultSetClosedHandler(v -> sqlRowStream.moreResults())
                                .handler(jsonArrays::add)
                                .endHandler(e -> result.complete(resultSetOf(jsonArrays, sqlRowStream)))
                                .exceptionHandler(result::fail);
                    } else {
                        result.fail(queryResult.cause());
                    }
                });

        context.runOnContext(v -> result.setHandler(resultHandler));
        return this;
    }

    @Override
    public SQLConnection queryStream(String query, Handler<AsyncResult<SQLRowStream>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(handler);

        return queryStreamWithParams(singletonList(query), null, null, handler);
    }

    @Override
    public SQLConnection queryStreamWithParams(String query, JsonArray params,
                                               Handler<AsyncResult<SQLRowStream>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(params);
        Objects.requireNonNull(handler);

        return queryStreamWithParams(singletonList(query), emptyListIfNull(params), null, handler);
    }

    private static <T> List<T> emptyListIfNull(T element) {
        return element == null ? Collections.EMPTY_LIST : Collections.singletonList(element);
    }

    @Override
    public SQLConnection queryStreamWithParams(String query, JsonArray params,
                                               Function<Row, JsonArray> rowMapper,
                                               Handler<AsyncResult<SQLRowStream>> handler) {
        Objects.requireNonNull(query);
        assert !query.isEmpty();
        Objects.requireNonNull(params);
        Objects.requireNonNull(rowMapper);
        Objects.requireNonNull(handler);

        return queryStreamWithParams(singletonList(query), emptyListIfNull(params), rowMapper, handler);
    }

    // TODO: Synchronise this call for doClose
    private SQLConnection queryStreamWithParams(List<String> queries, List<JsonArray> params,
                                                Function<Row, JsonArray> rowMapper,
                                                Handler<AsyncResult<SQLRowStream>> handler) {
        workerExecutor.executeBlocking((Handler<Future<SQLRowStream>>) future ->
                        executeQuery(queries, params, rowMapper, future),
                future -> executeQueryHandler(handler, future));
        return this;
    }

    private void executeQuery(List<String> queries, List<JsonArray> params,
                              Function<Row, JsonArray> rowMapper,
                              Future<SQLRowStream> future) {
        try {
            Statement statement = generateStatement(queries, params, session, sqlOptions, preparedStatementCache);
            com.datastax.driver.core.ResultSet resultSet = session.execute(statement);
            CassandraRowStream cassandraRowStream =
                    new CassandraRowStream(resultSet, workerExecutor, allRowStreams, rowMapper);
            allRowStreams.add(cassandraRowStream);
            future.complete(cassandraRowStream);
        } catch (Exception e) {
            future.fail(e);
        }
    }

    private void executeQueryHandler(Handler<AsyncResult<SQLRowStream>> handler,
                                     AsyncResult<SQLRowStream> blockingCallResult) {
        Future<SQLRowStream> result = Future.future();

        if (blockingCallResult.succeeded()) {
            result.complete(blockingCallResult.result());
        } else {
            result.fail(blockingCallResult.cause());
        }

        context.runOnContext(v -> result.setHandler(handler));
    }

    @Override
    public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
        Objects.requireNonNull(sql);
        assert !sql.isEmpty();
        Objects.requireNonNull(resultHandler);

        return queryWithParams(sql, null, null, context,
                future -> handleUpdate(resultHandler, context, future));
    }

    @Override
    public SQLConnection updateWithParams(String sql, JsonArray params,
                                          Handler<AsyncResult<UpdateResult>> resultHandler) {
        Objects.requireNonNull(sql);
        assert !sql.isEmpty();
        Objects.requireNonNull(params);
        Objects.requireNonNull(resultHandler);

        return queryWithParams(sql, params, null, context,
                future -> handleUpdate(resultHandler, context, future));
    }

    @Override
    public SQLConnection batch(List<String> sqlStatements,
                               Handler<AsyncResult<List<Integer>>> handler) {
        Objects.requireNonNull(sqlStatements);
        assert !sqlStatements.isEmpty();
        Objects.requireNonNull(handler);

        return queryStreamWithParams(sqlStatements, null, null,
                future -> handleBatch(sqlStatements.size(), handler, context, future));
    }

    @Override
    public SQLConnection batchWithParams(String sql, List<JsonArray> args,
                                         Handler<AsyncResult<List<Integer>>> handler) {
        Objects.requireNonNull(sql);
        assert !sql.isEmpty();
        Objects.requireNonNull(args);
        Objects.requireNonNull(handler);

        return queryStreamWithParams(singletonList(sql), args, null,
                future -> handleBatch(args.size(), handler, context, future));
    }

    @Override
    public SQLConnection batchWithParams(List<String> sqlStatements, List<JsonArray> args,
                                         Handler<AsyncResult<List<Integer>>> handler) {

        Objects.requireNonNull(sqlStatements);
        assert !sqlStatements.isEmpty();
        Objects.requireNonNull(args);
        Objects.requireNonNull(handler);
        assert sqlStatements.size() == args.size();

        return queryStreamWithParams(sqlStatements, args, null,
                future -> handleBatch(args.size(), handler, context, future));
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
        this.closeHandler = closeHandler;
        close();
    }

    @Override
    public SQLConnection setOptions(SQLOptions options) {
        this.sqlOptions = Objects.requireNonNull(options);
        if (sqlOptions.getQueryTimeout() < 1) {
            sqlOptions.setQueryTimeout(QUERY_TIME_OUT);
        }
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
