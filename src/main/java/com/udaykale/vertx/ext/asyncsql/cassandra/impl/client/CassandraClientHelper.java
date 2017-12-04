package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.SharedData;

import java.util.Map;

/**
 * @author uday
 */
final class CassandraClientHelper {

    private final Vertx vertx;

    CassandraClientHelper(Vertx vertx) {
        this.vertx = vertx;
    }

    CassandraClient getOrCreateCassandraClient(Cluster cluster, String keySpace, String clientName) {
        CassandraClient cassandraClient;

        synchronized (vertx) {
            SharedData sharedData = vertx.sharedData();
            String baseName = clientName + CassandraClient.class;
            Map<String, CassandraClient> sharedDataMap = sharedData.getLocalMap(baseName);
            cassandraClient = sharedDataMap.get(baseName);

            if (cassandraClient == null) {
                Session session = createSession(cluster, keySpace);
                WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor(clientName);
                Context context = vertx.getOrCreateContext();
                cassandraClient = new CassandraClientImpl(context, session, workerExecutor, clientName);
                sharedDataMap.put(baseName, cassandraClient);
            }
        }

        return cassandraClient;
    }

    private static Session createSession(Cluster cluster, String keySpace) {
        Session session;

        if (keySpace.isEmpty()) {
            session = cluster.connect();
        } else {
            session = cluster.connect(keySpace);
        }

        return session;
    }
}
