package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

/**
 * @author uday
 */
@RunWith(VertxUnitRunner.class)
public class CreationTest {

    private static final String KEY_SPACE = "KEY_SPACE";
    private static final String CLIENT_NAME = "CLIENT_NAME";

    private Vertx vertx;
    private Cluster cluster;
    private Session session;

    @Before
    public void before() {
        vertx = Vertx.vertx();
        cluster = Mockito.mock(Cluster.class);
        session = Mockito.mock(Session.class);
    }

    @Test
    public void createNonShared() {
        Mockito.when(cluster.connect()).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster);
        Assert.assertNotNull(cassandraClient);
    }

    @Test
    public void createNonSharedWithKeySpace() {
        Mockito.when(cluster.connect(KEY_SPACE)).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createNonShared(vertx, cluster, KEY_SPACE);
        Assert.assertNotNull(cassandraClient);
    }

    @Test
    public void createShared() {
        Mockito.when(cluster.connect()).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createShared(vertx, cluster);
        Assert.assertNotNull(cassandraClient);
    }

    @Test
    public void createSharedWithKeySpace() {
        Mockito.when(cluster.connect(KEY_SPACE)).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createShared(vertx, cluster, KEY_SPACE);
        Assert.assertNotNull(cassandraClient);
    }

    @Test
    public void createSharedWithClientName() {
        Mockito.when(cluster.connect()).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createShared(vertx, CLIENT_NAME, cluster);
        Assert.assertNotNull(cassandraClient);
    }

    @Test
    public void createSharedWithClientNameAndKeySpace() {
        Mockito.when(cluster.connect(KEY_SPACE)).thenReturn(session);
        CassandraClient cassandraClient = CassandraClient.createShared(vertx, CLIENT_NAME, cluster, KEY_SPACE);
        Assert.assertNotNull(cassandraClient);
    }
}
