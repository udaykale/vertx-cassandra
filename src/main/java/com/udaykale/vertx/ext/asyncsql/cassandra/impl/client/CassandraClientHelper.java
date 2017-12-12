package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.SharedData;

import java.util.Map;
import java.util.Objects;

/**
 * @author uday
 */
final class CassandraClientHelper {

    private final Vertx vertx;

    private CassandraClientHelper(Vertx vertx) {
        this.vertx = vertx;
    }

    static CassandraClientHelper instance(Vertx vertx) {
        Objects.requireNonNull(vertx);
        return new CassandraClientHelper(vertx);
    }

    CassandraClient getOrCreateCassandraClient(Cluster cluster, String keySpace, String clientName) {
        CassandraClient cassandraClient;

        synchronized (vertx) {
            SharedData sharedData = vertx.sharedData();
            String baseName = clientName + CassandraClient.class;
            Map<String, CassandraClient> sharedDataMap = sharedData.getLocalMap(baseName);
            cassandraClient = sharedDataMap.get(baseName);

            if (cassandraClient == null) {
                Context context = vertx.getOrCreateContext();
                WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor(clientName);
                Session session = keySpace.isEmpty() ? cluster.connect() : cluster.connect(keySpace);
                cassandraClient = CassandraClientImpl.of(clientName, context, session, workerExecutor);
                sharedDataMap.put(baseName, cassandraClient);
            }
        }

        return cassandraClient;
    }
}
