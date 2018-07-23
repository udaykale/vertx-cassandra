package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import com.datastax.driver.core.Cluster;
import com.udaykale.vertx.ext.asyncsql.cassandra.CassandraClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.Map;
import java.util.Objects;

public final class CassandraClientImpl implements CassandraClient {

    private final String clientName;
    private final ClientStateWrapper clientStateWrapper;

    private Handler<AsyncResult<Void>> closeHandler;

    private CassandraClientImpl(String clientName, ClientStateWrapper clientStateWrapper) {
        this.clientName = Objects.requireNonNull(clientName);
        this.clientStateWrapper = Objects.requireNonNull(clientStateWrapper);
    }

    public static CassandraClient getOrCreateCassandraClient(Vertx vertx, Cluster cluster, String keySpace,
                                                             String clientName) {
        return CassandraClientHelper.getOrCreateCassandraClient(vertx, cluster, keySpace, clientName);
    }

    // For synchronized over vertx
    final static class CassandraClientHelper {

        private final Vertx vertx;

        private CassandraClientHelper(Vertx vertx) {
            this.vertx = vertx;
        }

        static CassandraClient getOrCreateCassandraClient(Vertx vertx, Cluster cluster, String keySpace,
                                                          String clientName) {
            Objects.requireNonNull(vertx);
            CassandraClientHelper cassandraClientHelper = new CassandraClientHelper(vertx);
            return cassandraClientHelper.getOrCreateCassandraClient(cluster, keySpace, clientName);
        }

        private CassandraClient getOrCreateCassandraClient(Cluster cluster, String keySpace, String clientName) {
            CassandraClient cassandraClient;

            synchronized (vertx) {
                SharedData sharedData = vertx.sharedData();
                String baseName = clientName + CassandraClient.class;
                Map<String, CassandraClient> sharedDataMap = sharedData.getLocalMap(baseName);
                cassandraClient = sharedDataMap.get(baseName);

                if (cassandraClient == null) {
                    CassandraClientState clientState =
                            CreatingConnectionClientState.instance(vertx, cluster, keySpace, clientName);
                    ClientStateWrapper clientStateWrapper = ClientStateWrapper.of(clientState);

                    cassandraClient = new CassandraClientImpl(clientName, clientStateWrapper);
                    sharedDataMap.put(baseName, cassandraClient);
                }
            }

            return cassandraClient;
        }
    }

    @Override
    public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        Objects.requireNonNull(handler);
        synchronized (this) {
            clientStateWrapper.createConnection(handler);
        }
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> closeHandler) {
        this.closeHandler = Objects.requireNonNull(closeHandler);
        close();
    }

    @Override
    public void close() {
        synchronized (this) {
            clientStateWrapper.close(closeHandler);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraClientImpl that = (CassandraClientImpl) o;
        return Objects.equals(clientName, that.clientName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientName);
    }
}
