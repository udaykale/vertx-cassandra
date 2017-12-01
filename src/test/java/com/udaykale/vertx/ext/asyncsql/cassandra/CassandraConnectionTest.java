package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.datastax.driver.core.Cluster;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CassandraConnectionTest {

    private final Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
    private final SQLClient sqlClient = CassandraClient.createShared(Vertx.vertx(), "client1", cluster, "test");

    @Test
    public void query10000Elements(TestContext context) {
        String query = "SELECT * FROM emp LIMIT 10000";
        Async async = context.async();

        sqlClient.getConnection(connectionFuture -> {
            if (connectionFuture.failed()) {
                async.complete();
            } else {
                connectionFuture.result().query(query, ha -> {
                    if (ha.failed()) {
                        async.complete();
                    } else {
                        ResultSet resultSet = ha.result();
                        for (JsonObject jsonObject : resultSet.getRows()) {
                            System.out.println(jsonObject);
                        }
                        connectionFuture.result().close();
                        async.complete();
                    }
                });
            }
        });
    }
}
