**Vertx Casssandra Connector**
-
An implementation of vetx-sql-common for Cassandra

**Creating a connection:**

The connector has the following utilities for creating a Client object:

```java
public class Creator {

    private static final String KEY_SPACE = "KEY_SPACE";
    private static final String CLIENT_NAME = "CLIENT_NAME";
           
    private static final int port = 0; // Replace with correct Cassandra Port
    private static final String contactPoint = ""; // Replace with correct Cassandra Port
    
    private Vertx vertx = Vertx.vertx();
    private Cluster cluster = Cluster.builder()
      .addContactPoint(contactPoint)
      .withPort(port)
      .clusterBuilder
      .build();

    public void createClients() {
        CassandraClient cassandraClient1 = CassandraClient.createNonShared(vertx, cluster);
        CassandraClient cassandraClient2 = CassandraClient.createNonShared(vertx, cluster, KEY_SPACE);     
        CassandraClient cassandraClient3 = CassandraClient.createShared(vertx, cluster);
        CassandraClient cassandraClient4 = CassandraClient.createShared(vertx, cluster, KEY_SPACE);
        CassandraClient cassandraClient5 = CassandraClient.createShared(vertx, CLIENT_NAME, cluster);
        CassandraClient cassandraClient6 = CassandraClient.createShared(vertx, CLIENT_NAME, cluster, KEY_SPACE);
    }
}

```
`CassandraClient` implementation extends `SQLClient` from vetx-sql-common. For further sql operations refer [here](https://vertx.io/docs/vertx-sql-common/java/)
