package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

/**
 * @author uday
 */
@RunWith(VertxUnitRunner.class)
public class CreationArgumentTest {

    private static final String KEY_SPACE = "KEY_SPACE";
    private static final String CLIENT_NAME = "CLIENT_NAME";
    private static final String VERTX_OBJECT_CANNOT_BE_NULL = "Vertx object cannot be null";
    private static final String CLIENT_OBJECT_CANNOT_BE_NULL = "Client name cannot be null";
    private static final String CLUSTER_OBJECT_CANNOT_BE_NULL = "Cluster cannot be null";
    private static final String KEY_SPACE_OBJECT_CANNOT_BE_NULL = "KeySpace cannot be null";

    private Vertx vertx;
    private Cluster cluster;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        vertx = Vertx.vertx();
        cluster = Mockito.mock(Cluster.class);
    }

    @Test
    public void createNonSharedNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createNonShared(null, cluster);
    }

    @Test
    public void createNonSharedNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createNonShared(vertx, null);
    }

    @Test
    public void createNonSharedWitKeySpaceNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createNonShared(null, cluster, KEY_SPACE);
    }

    @Test
    public void createNonSharedWitKeySpaceNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createNonShared(vertx, null, KEY_SPACE);
    }

    @Test
    public void createNonSharedWithKeySpaceNullKeySpace() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(KEY_SPACE_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createNonShared(vertx, cluster, null);
    }

    @Test
    public void createSharedNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(null, cluster);
    }

    @Test
    public void createSharedNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, null);
    }

    @Test
    public void createSharedWithKeySpaceNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(null, cluster, KEY_SPACE);
    }

    @Test
    public void createSharedWithKeySpaceNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, null, KEY_SPACE);
    }

    @Test
    public void createSharedWithKeySpaceNullKeySpace() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(KEY_SPACE_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, cluster, null);
    }

    @Test
    public void createSharedWithClientNameNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(null, CLIENT_NAME, cluster);
    }

    @Test
    public void createSharedWithClientNameNullClientName() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLIENT_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, null, cluster);
    }

    @Test
    public void createSharedWithClientNameNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, CLIENT_NAME, null);
    }

    @Test
    public void createSharedWithClientNameAndKeySpaceNullVertx() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(VERTX_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(null, CLIENT_NAME, cluster, KEY_SPACE);
    }

    @Test
    public void createSharedWithClientNameAndKeySpaceNullClientName() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLIENT_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, null, cluster, KEY_SPACE);
    }

    @Test
    public void createSharedWithClientNameAndKeySpaceNullCluster() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(CLUSTER_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, CLIENT_NAME, null, KEY_SPACE);
    }

    @Test
    public void createSharedWithClientNameAndKeySpaceNullKeySpace() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(KEY_SPACE_OBJECT_CANNOT_BE_NULL);
        CassandraClient.createShared(vertx, CLIENT_NAME, cluster, null);
    }
}
