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
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraStatementUtil.generateStatement;

/**
 * @author uday
 */
class ConnectionStreamState implements CassandraConnectionState {

    private ConnectionStreamState() {
    }

    static ConnectionStreamState instance() {
        return new ConnectionStreamState();
    }

    @Override
    public void close(ConnectionInfoWrapper connectionInfoWrapper, CassandraConnection connection,
                      Handler<AsyncResult<Void>> closeHandler) {
        connectionInfoWrapper.setState(CloseConnectionState.instance());
        connectionInfoWrapper.getAllOpenConnections().remove(connection);

        for (SQLRowStream sqlRowStream : connectionInfoWrapper.getAllRowStreams()) {
            sqlRowStream.close();
        }

        if (closeHandler != null) {
            Context context = connectionInfoWrapper.getContext();
            context.runOnContext(v -> closeHandler.handle(Future.succeededFuture()));
        }
    }

    @Override
    public void stream(ConnectionInfoWrapper connectionInfoWrapper, List<String> queries, List<JsonArray> params,
                       Function<Row, JsonArray> rowMapper, Handler<AsyncResult<SQLRowStream>> handler) {
        Context context = connectionInfoWrapper.getContext();

        connectionInfoWrapper.getWorkerExecutor().executeBlocking((Handler<Future<SQLRowStream>>) future ->
                        executeQueryAndStream(connectionInfoWrapper, queries, params, rowMapper, future),
                future -> handlerForExecuteQueryAndStream(handler, future, context));
    }

    private static void executeQueryAndStream(ConnectionInfoWrapper connectionInfoWrapper, List<String> queries,
                                              List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                                              Future<SQLRowStream> future) {
        try {
            Session session = connectionInfoWrapper.getSession();
            SQLOptions sqlOptions = connectionInfoWrapper.getSqlOptions();
            WorkerExecutor workerExecutor = connectionInfoWrapper.getWorkerExecutor();
            Set<SQLRowStream> allRowStreams = connectionInfoWrapper.getAllRowStreams();
            Map<String, PreparedStatement> preparedStatementCache = connectionInfoWrapper.getPreparedStatementCache();

            // generate cassandra CQL statement from given params
            Statement statement = generateStatement(queries, params, session, preparedStatementCache);
            // apply SQL options
            applySQLOptions(statement, sqlOptions);
            // execute statement
            com.datastax.driver.core.ResultSet resultSet = session.execute(statement);
            // create a row stream from the result set
            int rowStreamId = connectionInfoWrapper.generateStreamId();
            Context context = connectionInfoWrapper.getContext();
            CassandraRowStreamImpl cassandraRowStream = CassandraRowStreamImpl.of(rowStreamId,
                    resultSet, workerExecutor, allRowStreams, rowMapper, context);
            // add this new stream to list of already created streams
            allRowStreams.add(cassandraRowStream);
            future.complete(cassandraRowStream);
        } catch (Throwable t) {
            future.fail(t);
        }
    }

    private static void applySQLOptions(Statement statement, SQLOptions sqlOptions) {
        int queryTimeOut = sqlOptions.getQueryTimeout();
        statement.setReadTimeoutMillis(queryTimeOut);
    }

    private static void handlerForExecuteQueryAndStream(Handler<AsyncResult<SQLRowStream>> handler,
                                                        AsyncResult<SQLRowStream> blockingCallResult,
                                                        Context context) {
        Future<SQLRowStream> result = Future.future();

        if (blockingCallResult.failed()) {
            result.fail(blockingCallResult.cause());
        } else {
            result.complete(blockingCallResult.result());
        }

        context.runOnContext(v -> result.setHandler(handler));
    }
}
