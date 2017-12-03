package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.CassandraRowStreamImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraStatementHelper.generateStatement;

/**
 * @author uday
 */
final class CassandraConnectionStreamHelper {

    private final CassandraConnection cassandraConnection;

    CassandraConnectionStreamHelper(CassandraConnection cassandraConnection) {
        this.cassandraConnection = cassandraConnection;
    }

    public void queryStreamWithParams(ConnectionInfo connectionInfo, List<String> queries, List<JsonArray> params,
                                      Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler) {
        WorkerExecutor workerExecutor = connectionInfo.getWorkerExecutor();
        Context context = connectionInfo.getContext();

        synchronized (cassandraConnection) {
            if (connectionInfo.isConnected()) {
                // check if connection is closed
                workerExecutor.executeBlocking((Handler<Future<SQLRowStream>>) future ->
                                executeQueryAndStream(connectionInfo, queries, params, rowMapper, future),
                        future -> executeQueryAndStreamHandler(handler, future, context));
            } else {
                // connection is closed, do nothing
            }
        }
    }

    private static void executeQueryAndStream(ConnectionInfo connectionInfo, List<String> queries,
                                              List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                                              Future<SQLRowStream> future) {
        try {
            Session session = connectionInfo.getSession();
            SQLOptions sqlOptions = connectionInfo.getSqlOptions();
            AtomicInteger rowStreamId = connectionInfo.getRowStreamId();
            WorkerExecutor workerExecutor = connectionInfo.getWorkerExecutor();
            Set<SQLRowStream> allRowStreams = connectionInfo.getAllRowStreams();
            Map<String, PreparedStatement> preparedStatementCache = connectionInfo.getPreparedStatementCache();

            // generate cassandra CQL statement from given params
            Statement statement = generateStatement(queries, params, session, sqlOptions, preparedStatementCache);
            //execute statement
            com.datastax.driver.core.ResultSet resultSet = session.execute(statement);
            // create a row stream from the result set
            CassandraRowStreamImpl cassandraRowStream = new CassandraRowStreamImpl(rowStreamId.getAndIncrement(),
                    resultSet, workerExecutor, allRowStreams, rowMapper);
            // add this new stream to list of already created streams
            allRowStreams.add(cassandraRowStream);
            future.complete(cassandraRowStream);
        } catch (Exception e) {
            future.fail(e);
        }
    }

    private static void executeQueryAndStreamHandler(Handler<AsyncResult<SQLRowStream>> handler,
                                                     AsyncResult<SQLRowStream> blockingCallResult,
                                                     Context context) {
        Future<SQLRowStream> result = Future.future();

        if (blockingCallResult.succeeded()) {
            result.complete(blockingCallResult.result());
        } else {
            result.fail(blockingCallResult.cause());
        }

        context.runOnContext(v -> result.setHandler(handler));
    }
}
