package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.shareddata.SharedData;

import java.util.Map;
import java.util.Objects;

final public class CassandraClientHelper {

    private final Vertx vertx;

    private CassandraClientHelper(Vertx vertx) {
        this.vertx = vertx;
    }

    public static CassandraClient getOrCreateCassandraClient(Vertx vertx, Cluster cluster, String keySpace,
                                                      String clientName) {
        Objects.requireNonNull(vertx);

        CassandraClientHelper cassandraClientHelper = new CassandraClientHelper(vertx);

        return cassandraClientHelper.getOrCreateCassandraClient(cluster, keySpace, clientName);
    }

    private CassandraClient getOrCreateCassandraClient(Cluster cluster, String keySpace, String clientName) {
        CassandraClient cassandraClient;

        // For synchronized over vertx
        synchronized (vertx) {
            SharedData sharedData = vertx.sharedData();
            String baseName = clientName + CassandraClient.class;
            Map<String, CassandraClient> sharedDataMap = sharedData.getLocalMap(baseName);
            cassandraClient = sharedDataMap.get(baseName);

            if (cassandraClient == null) {
                Context context = vertx.getOrCreateContext();
                WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor(clientName);
                CassandraClientState clientState =
                        CreatingConnectionClientState.instance(context, cluster, keySpace, workerExecutor);
                ClientStateWrapper clientStateWrapper = ClientStateWrapper.of(clientState);

                cassandraClient = new CassandraClientImpl(context, clientName, clientStateWrapper);
                sharedDataMap.put(baseName, cassandraClient);
            }
        }

        return cassandraClient;
    }
}
