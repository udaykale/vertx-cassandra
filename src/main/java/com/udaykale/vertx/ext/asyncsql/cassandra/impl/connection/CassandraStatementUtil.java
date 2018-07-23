package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.vertx.core.json.JsonArray;

import java.util.List;
import java.util.Map;

final class CassandraStatementUtil {

    private CassandraStatementUtil() {
    }

    static Statement generateStatement(List<String> queries, List<JsonArray> params, Session session,
                                       Map<String, PreparedStatement> psCache) {
        boolean isSimpleStatement = singleQueryAndNonNullParams(queries, params) && params.isEmpty();
        boolean isPreparedStatement = singleQueryAndNonNullParams(queries, params) && params.size() == 1;
        boolean isSingleBatchStatement = singleQueryAndNonNullParams(queries, params) && params.size() > 1;
        boolean isMultiBatchStatement = params != null && params.size() == queries.size() && params.size() > 1;

        Statement statement;

        if (isSimpleStatement) {
            statement = generateSimpleStatement(queries.get(0));
        } else if (isPreparedStatement) {
            statement = generateBoundStatement(queries.get(0), params.get(0), session, psCache);
        } else if (isSingleBatchStatement) {
            statement = generateSingleBatchStatement(queries.get(0), params, session, psCache);
        } else if (isMultiBatchStatement) {
            statement = generateMultiBatchStatement(queries, params, session, psCache);
        } else {
            throw new IllegalArgumentException("Don't know what you want to do with these parameters");
        }

        return statement;
    }

    private static boolean singleQueryAndNonNullParams(List<String> queries, List<JsonArray> params) {
        return queries.size() == 1 && queries.get(0) != null && params != null;
    }

    private static SimpleStatement generateSimpleStatement(String query) {
        return new SimpleStatement(query);
    }

    private static BoundStatement generateBoundStatement(String query, JsonArray params, Session session,
                                                         Map<String, PreparedStatement> psCache) {
        PreparedStatement preparedStatement = psCache.computeIfAbsent(query, session::prepare);
        return preparedStatement.bind(params.stream().toArray());
    }

    private static BatchStatement generateSingleBatchStatement(String query, List<JsonArray> params, Session session,
                                                               Map<String, PreparedStatement> psCache) {
        BatchStatement batchStatement = new BatchStatement();

        for (JsonArray param : params) {
            Statement statement = generateBoundStatement(query, param, session, psCache);
            batchStatement.add(statement);
        }

        return batchStatement;
    }

    private static BatchStatement generateMultiBatchStatement(List<String> queries, List<JsonArray> params,
                                                              Session session, Map<String, PreparedStatement> psCache) {
        BatchStatement batchStatement = new BatchStatement();

        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            JsonArray param = params.get(i);
            Statement statement;

            if (param == null || param.isEmpty()) {
                statement = generateSimpleStatement(query);
            } else {
                statement = generateBoundStatement(query, param, session, psCache);
            }

            batchStatement.add(statement);
        }

        return batchStatement;
    }
}
