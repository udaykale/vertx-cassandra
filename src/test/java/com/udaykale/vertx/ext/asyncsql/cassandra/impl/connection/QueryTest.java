package com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.List;

/**
 * @author uday
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest {

    private static final String QUERY = "QUERY";

    @Test
    public void queryTest(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();

        Row row = Mockito.mock(Row.class);
        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = Mockito.mock(Session.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        PagingState pagingState = Mockito.mock(PagingState.class);
        ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
        ColumnDefinitions columnDefinitions = Mockito.mock(ColumnDefinitions.class);

        Mockito.when(resultSet.one()).thenReturn(row);
        Mockito.when(cluster.connect()).thenReturn(session);
        Mockito.when(columnDefinitions.size()).thenReturn(3);
        Mockito.when(resultSet.getExecutionInfo()).thenReturn(executionInfo);
        Mockito.when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        Mockito.when(session.execute(Mockito.any(Statement.class))).thenReturn(resultSet);
        Mockito.when(resultSet.getAvailableWithoutFetching()).thenReturn(1000, 990, 90);
        Mockito.when(executionInfo.getPagingState()).thenReturn(pagingState, pagingState, pagingState, null);
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
//                        context.assertEquals(2080, rs.size());
                        async.complete();
                    }
                });
            }
        });
    }
}
