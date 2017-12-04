package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.datastax.driver.core.Cluster;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientImpl;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.sql.SQLClient;

import java.util.Objects;
import java.util.UUID;

/**
 * @author uday
 */
public interface CassandraClient extends SQLClient, Shareable {

    String DEFAULT_CLIENT_NAME = "DEFAULT_CASSANDRA_CLIENT_NAME";

    /**
     * Create a Cassandra client which maintains its own session. The client is not shared.
     *
     * @param vertx   the Vertx instance
     * @param cluster cassandra cluster
     * @return the client
     */
    static CassandraClient createNonShared(Vertx vertx, Cluster cluster) {
        return createShared(vertx, UUID.randomUUID().toString(), cluster, "");
    }

    /**
     * Create a Cassandra client which maintains its own session. The client is not shared.
     *
     * @param vertx    the Vertx instance
     * @param cluster  cassandra cluster
     * @param keySpace cassandra key space
     * @return the client
     */
    static CassandraClient createNonShared(Vertx vertx, Cluster cluster, String keySpace) {
        return createShared(vertx, UUID.randomUUID().toString(), cluster, keySpace);
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx   the Vertx instance
     * @param cluster cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, Cluster cluster) {
        return createShared(vertx, DEFAULT_CLIENT_NAME, cluster, "");
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx    the Vertx instance
     * @param cluster  cassandra cluster
     * @param keySpace cassandra key space
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, Cluster cluster, String keySpace) {
        return createShared(vertx, DEFAULT_CLIENT_NAME, cluster, keySpace);
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx      the Vertx instance
     * @param clientName the pool name
     * @param cluster    cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, String clientName, Cluster cluster) {
        return createShared(vertx, clientName, cluster, "");
    }

    /**
     * Create a Cassandra client which shares its data source with any other Cassandra clients created with the same
     * data source name
     *
     * @param vertx      the Vertx instance
     * @param clientName the pool name
     * @param keySpace   casandra key space
     * @param cluster    cassandra cluster
     * @return the client
     */
    static CassandraClient createShared(Vertx vertx, String clientName, Cluster cluster, String keySpace) {
        Objects.requireNonNull(vertx, "Vertx object cannot be null");
        Objects.requireNonNull(clientName, "Client name cannot be null");
        Objects.requireNonNull(cluster, "Cluster cannot be null");
        Objects.requireNonNull(keySpace, "KeySpace cannot be null");
        return CassandraClientImpl.getOrCreateCassandraClient(vertx, cluster, keySpace, clientName);
    }
}
