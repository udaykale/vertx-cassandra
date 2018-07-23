package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(VertxUnitRunner.class)
public class ClientConnectionTest {
    private CassandraClient cassandraClient;

    @Rule
    public ExpectedException illegalState = ExpectedException.none();

    @Before
    public void before() {
        Vertx vertx = Vertx.vertx();
        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(cluster.connect()).thenReturn(session);
        cassandraClient = CassandraClient.createShared(vertx, cluster);
    }

    @Test
    public void getConnection(TestContext context) {
        Async async = context.async();
        cassandraClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                context.fail();
            }
            async.complete();
        });
    }

    @Test
    public void getConnectionWhenClientClosed(TestContext context) {
        Async async = context.async();

        cassandraClient.close(future -> {
            if (future.failed()) {
                context.fail();
                async.complete();
            } else {
                cassandraClient.getConnection(connectionFuture -> {
                    if (connectionFuture.failed()) {
                        Throwable cause = connectionFuture.cause();
                        context.assertEquals(IllegalStateException.class, cause.getClass());
                        context.assertEquals("Cannot create connection when client is already closed", cause.getMessage());
                    } else {
                        context.fail();
                    }
                    async.complete();
                });
            }
        });
    }

    @Test
    public void closeClient(TestContext context) {
        Async async = context.async();
        try {
            cassandraClient.close();
        } catch (Exception e) {
            context.fail("No exception was expected during client");
        }
        async.complete();
    }

    @Test
    public void closeClientWithHandler(TestContext context) {
        Async async = context.async();
        try {
            cassandraClient.close(closeHandler -> {
            });
        } catch (Exception e) {
            context.fail("No exception was expected during client");
        }
        async.complete();
    }

    @Test
    public void getConnectionThenCloseClient(TestContext context) {
        Async async = context.async();
        cassandraClient.getConnection(future -> {
            if (future.failed()) {
                context.fail();
                async.complete();
            } else {
                cassandraClient.close(closeHandler -> async.complete());
            }
        });
    }

    @Test
    public void closeWhenClientAlreadyClosedAndSecondCloseHandlerIsNull(TestContext context) throws InterruptedException {
        Async async = context.async();
        cassandraClient.close();
        Thread.sleep(2000);

        try {
            cassandraClient.close();
            context.fail("Close method should have thrown an exception");
        } catch (Exception e) {
            Assert.assertEquals("Cannot re-close client when it is already closed", e.getMessage());
        }

        async.complete();
    }

    @Test
    public void closeWhenClientAlreadyClosed(TestContext context) throws InterruptedException {
        Async async = context.async();
        cassandraClient.close();
        Thread.sleep(2000);

        try {
            cassandraClient.close(resultFuture -> {
                if (resultFuture.failed()) {
                    String message = resultFuture.cause().getMessage();
                    Assert.assertEquals("Cannot re-close client when it is already closed", message);
                } else {
                    context.fail();
                }
                async.complete();
            });
        } catch (Exception e) {
            Assert.assertEquals("Cannot re-close client when it is already closed", e.getMessage());
        }
    }
}
