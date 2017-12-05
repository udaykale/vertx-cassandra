package com.udaykale.vertx.ext.asyncsql.cassandra.client;

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

import java.util.Objects;

@RunWith(VertxUnitRunner.class)
public class EqualsAndHashCodeTest {

    private static final String CLIENT = "client";

    private Vertx vertx;
    private Cluster cluster;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        cluster = Mockito.mock(Cluster.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(cluster.connect()).thenReturn(session);
    }

    @Test
    public void equalsForSameClientObjectsTest() {
        CassandraClient cassandraClient1 = CassandraClient.createShared(vertx, "test1", cluster);
        CassandraClient cassandraClient2 = CassandraClient.createShared(vertx, "test1", cluster);
        Assert.assertTrue(cassandraClient1.equals(cassandraClient2));
    }

    @Test
    public void equalsForObjectsOfDifferentTypesTest() {
        CassandraClient cassandraClient = CassandraClient.createShared(vertx, "test1", cluster);
        Assert.assertFalse(cassandraClient.equals(""));
    }

    @Test
    public void equalsForDifferentObjectsTest() {
        CassandraClient cassandraClient1 = CassandraClient.createShared(vertx, "test1", cluster);
        CassandraClient cassandraClient2 = CassandraClient.createShared(vertx, "test2", cluster);
        Assert.assertNotEquals(cassandraClient1, cassandraClient2);
    }

    @Test
    public void hashCodeTest() {
        CassandraClient cassandraClient1 = CassandraClient.createShared(vertx, CLIENT, cluster);
        Assert.assertEquals(cassandraClient1.hashCode(), Objects.hash(CLIENT));
    }
}
