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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraStatementUtil.generateStatement;

class ConnectionStreamState implements CassandraConnectionState {

    private final Context context;
    private final Session session;
    private final AtomicInteger rowStreamId;
    private final WorkerExecutor workerExecutor;
    private final Set<SQLRowStream> allRowStreams;
    private final Set<CassandraConnection> allOpenConnections;
    private final Map<String, PreparedStatement> preparedStatementCache;

    private ConnectionStreamState(Context context, WorkerExecutor workerExecutor, Session session,
                                  Map<String, PreparedStatement> preparedStatementCache, AtomicInteger rowStreamId,
                                  Set<CassandraConnection> allOpenConnections, Set<SQLRowStream> allRowStreams) {
        this.context = context;
        this.session = session;
        this.rowStreamId = rowStreamId;
        this.allRowStreams = allRowStreams;
        this.workerExecutor = workerExecutor;
        this.allOpenConnections = allOpenConnections;
        this.preparedStatementCache = preparedStatementCache;
    }

    static ConnectionStreamState instance(Context context, WorkerExecutor workerExecutor, Set<SQLRowStream> allRowStreams,
                                          Session session, Map<String, PreparedStatement> preparedStatementCache,
                                          AtomicInteger rowStreamId, Set<CassandraConnection> allOpenConnections) {
        return new ConnectionStreamState(context, workerExecutor, session, preparedStatementCache,
                rowStreamId, allOpenConnections, allRowStreams);
    }

    @Override
    public void close(ConnectionStateWrapper connectionStateWrapper, CassandraConnection connection,
                      Handler<AsyncResult<Void>> closeHandler) {
        connectionStateWrapper.setState(CloseConnectionState.instance());
        allOpenConnections.remove(connection);
        Iterator<SQLRowStream> iterator = allRowStreams.iterator();

        recursiveClose(iterator, future -> {
            if (closeHandler != null) {
                closeHandler.handle(Future.succeededFuture());
            }
        });
    }

    private void recursiveClose(Iterator<SQLRowStream> iterator, Handler<AsyncResult<Void>> handler) {
        if (iterator.hasNext()) {
            SQLRowStream sqlRowStream = iterator.next();
            sqlRowStream.close(future -> recursiveClose(iterator, handler));
        } else {
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public void stream(ConnectionStateWrapper connectionStateWrapper, List<String> queries, List<JsonArray> params,
                       SQLOptions sqlOptions, Function<Row, JsonArray> rowMapper,
                       Handler<AsyncResult<SQLRowStream>> handler) {
        workerExecutor.executeBlocking((Handler<Future<SQLRowStream>>) future ->
                executeQueryAndStream(queries, params, rowMapper, sqlOptions, future), fut -> {
            if (fut.failed()) {
                handler.handle(Future.failedFuture(fut.cause()));
            } else {
                handler.handle(Future.succeededFuture(fut.result()));
            }
        });
    }

    private void executeQueryAndStream(List<String> queries, List<JsonArray> params, Function<Row, JsonArray> rowMapper,
                                       SQLOptions sqlOptions, Future<SQLRowStream> future) {
        try {
            // generate cassandra CQL statement from given params
            Statement statement = generateStatement(queries, params, session, preparedStatementCache);
            // apply SQL options
            applySQLOptions(statement, sqlOptions);
            // execute statement
            com.datastax.driver.core.ResultSet resultSet = session.execute(statement);
            // create a row stream from the result set
            CassandraRowStreamImpl cassandraRowStream = CassandraRowStreamImpl.of(rowStreamId.getAndIncrement(),
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
}
