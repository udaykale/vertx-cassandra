package com.udaykale.vertx.ext.asyncsql.cassandra.impl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.List;
import java.util.Map;

import static java.util.Collections.nCopies;

final class CassandraConnectionHelper {

    static ResultSet resultSetOf(List<JsonArray> jsonArrays, SQLRowStream sqlRowStream) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnNames(sqlRowStream.columns());
        resultSet.setResults(jsonArrays);
        return resultSet;
    }

    static Statement generateStatement(List<String> queries, List<JsonArray> params,
                                       Session session, SQLOptions sqlOptions,
                                       Map<String, PreparedStatement> preparedStatementCache) {
        boolean isSimpleStatement = queries.size() == 1 && queries.get(0) != null && params != null && params.isEmpty();
        boolean isPreparedStatement = queries.size() == 1 && queries.get(0) != null && params != null && params.size() == 1;
        boolean isSingleBatchStatement = queries.size() == 1 && queries.get(0) != null && params != null && !params.isEmpty();
        boolean isMultiBatchStatement = params != null && params.size() == queries.size();

        Statement statement;

        if (isPreparedStatement) {
            statement = generateBoundStatement(queries.get(0), params.get(0), session, sqlOptions, preparedStatementCache);
        } else if (isSimpleStatement) {
            statement = generateSimpleStatement(queries.get(0), sqlOptions);
        } else if (isSingleBatchStatement) {
            statement = generateSingleBatchStatement(queries.get(0), params, session, sqlOptions, preparedStatementCache);
        } else if (isMultiBatchStatement) {
            statement = generateMultiBatchStatement(queries, params, session, sqlOptions, preparedStatementCache);
        } else {
            throw new IllegalArgumentException("Don't know what you want to do with these parameters");
        }

        return statement;
    }

    private static SimpleStatement generateSimpleStatement(String query, SQLOptions sqlOptions) {
        SimpleStatement simpleStatement = new SimpleStatement(query);
        simpleStatement.setReadTimeoutMillis(sqlOptions.getQueryTimeout());
        return simpleStatement;
    }

    private static BoundStatement generateBoundStatement(String query, JsonArray params,
                                                         Session session, SQLOptions sqlOptions,
                                                         Map<String, PreparedStatement> preparedStatementCache) {
        PreparedStatement preparedStatement = preparedStatementCache.get(query);

        if (preparedStatement == null) {
            preparedStatement = session.prepare(query);
            preparedStatementCache.put(query, preparedStatement);
        }

        BoundStatement boundStatement = preparedStatement.bind();
        boundStatement.bind(params.stream().toArray());
        boundStatement.setReadTimeoutMillis(sqlOptions.getQueryTimeout());
        return boundStatement;
    }

    private static BatchStatement generateSingleBatchStatement(String query, List<JsonArray> params, Session session,
                                                               SQLOptions sqlOptions, Map<String, PreparedStatement> preparedStatementCache) {
        BatchStatement batchStatement = new BatchStatement();

        for (JsonArray param : params) {
            Statement statement = generateBoundStatement(query, param, session, sqlOptions, preparedStatementCache);
            batchStatement.add(statement);
        }

        return batchStatement;
    }

    private static BatchStatement generateMultiBatchStatement(List<String> queries, List<JsonArray> params, Session session,
                                                              SQLOptions sqlOptions, Map<String, PreparedStatement> preparedStatementCache) {
        BatchStatement batchStatement = new BatchStatement();

        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            JsonArray param = params.get(i);
            Statement statement;

            if (param == null || param.isEmpty()) {
                statement = generateBoundStatement(query, param, session, sqlOptions, preparedStatementCache);
            } else {
                statement = generateSimpleStatement(query, sqlOptions);
            }

            batchStatement.add(statement);
        }

        return batchStatement;
    }

    static void handleUpdate(Handler<AsyncResult<UpdateResult>> resultHandler,
                             Context context, AsyncResult<ResultSet> future) {
        Future<UpdateResult> result;

        if (future.succeeded()) {
            UpdateResult updateResult = new UpdateResult();
            updateResult.setUpdated(-1);
            result = Future.succeededFuture(updateResult);
        } else {
            result = Future.failedFuture(future.cause());
        }

        context.runOnContext(v -> result.setHandler(resultHandler));
    }

    static void handleBatch(int resultSize, Handler<AsyncResult<List<Integer>>> handler,
                            Context context, AsyncResult<SQLRowStream> future) {
        Future<List<Integer>> result;

        if (future.succeeded()) {
            result = Future.succeededFuture(nCopies(resultSize, -1));
        } else {
            result = Future.failedFuture(future.cause());
        }

        context.runOnContext(v -> result.setHandler(handler));
    }

    static void handleQuery(Handler<AsyncResult<Void>> resultHandler,
                            AsyncResult<ResultSet> future, Context context) {
        Future<Void> result = Future.future();

        if (future.succeeded()) {
            result.complete();
        } else {
            result.fail(future.cause());
        }

        context.runOnContext(v -> result.setHandler(resultHandler));
    }
}
