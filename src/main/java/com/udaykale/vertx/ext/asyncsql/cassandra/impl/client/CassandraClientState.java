package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLConnection;

/**
 * @author uday
 */
interface CassandraClientState {

    void close(ClientInfo clientInfo);

    void createConnection(ClientInfo clientInfo, Handler<AsyncResult<SQLConnection>> handler);
}
