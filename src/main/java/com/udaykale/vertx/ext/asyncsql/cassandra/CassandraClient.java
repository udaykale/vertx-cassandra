package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.datastax.driver.core.Cluster;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientImpl;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.sql.SQLClient;

import java.util.UUID;

/**
 * @author uday
 */
public interface CassandraClient extends SQLClient, Shareable {

    String DEFAULT_CLIENT_NAME = "DEFAULT_CASSANDRA_CLIENT_NAME";

    /**
     * Create a Cassandra client which maintains its own session. The client is not shared.
     *
     * @param vertx   the Vert.x instance
     * @param cluster cassandra cluster
     * @return the client
     */
    static CassandraClient createNonShared(Vertx vertx, Cluster cluster) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, "", UUID.randomUUID().toString());
    }

    /**
     * Create a Cassandra client which maintains its own session. The client is not shared.
     *
     * @param vertx    the Vert.x instance
     * @param cluster  cassandra cluster
     * @param keySpace cassandra key space
     * @return the client
     */
    static CassandraClient createNonShared(Vertx vertx, Cluster cluster, String keySpace) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, keySpace, UUID.randomUUID().toString());
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx      the Vert.x instance
     * @param clientName the pool name
     * @param cluster    cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, String clientName, Cluster cluster) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, "", clientName);
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx      the Vert.x instance
     * @param clientName the pool name
     * @param keySpace   casandra key space
     * @param cluster    cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, String clientName, Cluster cluster, String keySpace) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, keySpace, clientName);
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx   the Vert.x instance
     * @param cluster cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, Cluster cluster) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, "", DEFAULT_CLIENT_NAME);
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx    the Vert.x instance
     * @param cluster  cassandra cluster
     * @param keySpace cassandra key space
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, Cluster cluster, String keySpace) {
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, keySpace, DEFAULT_CLIENT_NAME);
    }
}
