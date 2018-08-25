package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraConnection;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.List;

@RunWith(VertxUnitRunner.class)
public class ConnectionQueryTest {

    private static final String QUERY = "QUERY";

    private Row row;
    private Vertx vertx;
    private Cluster cluster;
    private Session session;
    private ResultSet resultSet;
    private PagingState pagingState;
    private ExecutionInfo executionInfo;
    private ColumnDefinitions columnDefinitions;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        row = Mockito.mock(Row.class);
        cluster = Mockito.mock(Cluster.class);
        session = Mockito.mock(Session.class);
        resultSet = Mockito.mock(ResultSet.class);
        pagingState = Mockito.mock(PagingState.class);
        executionInfo = Mockito.mock(ExecutionInfo.class);
        columnDefinitions = Mockito.mock(ColumnDefinitions.class);

        Mockito.when(resultSet.one()).thenReturn(row);
        Mockito.when(cluster.connect()).thenReturn(session);
        Mockito.when(resultSet.getExecutionInfo()).thenReturn(executionInfo);
        Mockito.when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        Mockito.when(session.execute(Mockito.any(Statement.class))).thenReturn(resultSet);
    }

    @Test
    public void executeTest(TestContext context) {
        Async async = context.async();

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.execute(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void executeWithValidSqlTimeOutTest(TestContext context) {
        Async async = context.async();

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.execute(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                }).setOptions(new SQLOptions().setQueryTimeout(100));
            }
        });
    }

    @Test
    public void executeWithNullSqlOptionsTest(TestContext context) {
        Async async = context.async();

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.execute(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        async.complete();
                    } else {
                        context.fail();
                        async.complete();
                    }
                }).setOptions(null);
            }
        });
    }

    @Test
    public void queryTest(TestContext context) {
        Async async = context.async();

        Mockito.when(columnDefinitions.size()).thenReturn(3);
        Mockito.when(resultSet.getAvailableWithoutFetching()).thenReturn(1000, 990);
        Mockito.when(executionInfo.getPagingState()).thenReturn(pagingState, pagingState, null);
        Mockito.when(row.getObject(0)).thenReturn("123");
        Mockito.when(row.getObject(1)).thenReturn("123");
        Mockito.when(row.getObject(2)).thenReturn("123");

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.query(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        List<JsonArray> rs = queryFuture.result().getResults();
                        context.assertEquals(1890, rs.size());
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void queryWithParamsTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.queryWithParams(QUERY, new JsonArray(), queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void queryWithParamsAndRowMapperTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                CassandraConnection cassandraConnection = (CassandraConnection) connectionFuture.result();
                cassandraConnection.queryWithParams(QUERY, new JsonArray(), x -> null, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                    }
                    async.complete();
                });
            }
        });
    }

    @Test
    public void queryStreamTest(TestContext context) {
        Async async = context.async();

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.queryStream(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void queryStreamWithParamsTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.queryStreamWithParams(QUERY, new JsonArray(), queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void queryStreamWithParamsAndRowMapperTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                CassandraConnection sqlConnection = (CassandraConnection) connectionFuture.result();
                sqlConnection.queryStreamWithParams(QUERY, new JsonArray(), x -> null, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void updateTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.update(QUERY, queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }

    @Test
    public void updateWithParamsTest(TestContext context) {
        Async async = context.async();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(session.prepare(Mockito.any(String.class))).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.bind(Mockito.any(Object[].class))).thenReturn(boundStatement);
        Mockito.when(boundStatement.setReadTimeoutMillis(Mockito.any(Integer.class))).thenReturn(statement);

        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);

        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
                async.complete();
            } else {
                SQLConnection sqlConnection = connectionFuture.result();
                sqlConnection.updateWithParams(QUERY, new JsonArray(), queryFuture -> {
                    if (queryFuture.failed()) {
                        context.fail();
                        async.complete();
                    } else {
                        async.complete();
                    }
                });
            }
        });
    }
}
