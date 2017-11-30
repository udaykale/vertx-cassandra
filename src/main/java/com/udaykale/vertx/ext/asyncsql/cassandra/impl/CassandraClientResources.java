package com.udaykale.vertx.ext.asyncsql.cassandra.impl;

import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author uday
 */
final class CassandraClientResources {

    private final Set<SQLConnection> allConnections;
    private final Map<SQLConnection, Set<SQLRowStream>> allStreams;

    CassandraClientResources() {
        this.allConnections = new ConcurrentSkipListSet<>();
        this.allStreams = new ConcurrentHashMap<>();
    }

    void closeClient() {
        for (SQLConnection sqlConnection : allConnections) {
            sqlConnection.close();
        }
    }

    void closeConnection(SQLConnection sqlConnection) {
        Set<SQLRowStream> sqlRowStreams = allStreams.get(sqlConnection);

        for (SQLRowStream sqlRowStream : sqlRowStreams) {
            sqlRowStream.close();
        }

        allStreams.remove(sqlConnection);
    }

    void closeStream(SQLConnection sqlConnection, SQLRowStream sqlRowStream) {
        allStreams.get(sqlConnection).remove(sqlRowStream);
    }

    void addConnection(SQLConnection sqlConnection) {
        allConnections.add(sqlConnection);
    }

    void addStream(SQLConnection sqlConnection, SQLRowStream sqlRowStream) {
        Set<SQLRowStream> sqlRowStreams = allStreams.get(sqlConnection);
        if (sqlRowStreams == null) {
            sqlRowStreams = new ConcurrentSkipListSet<>();
            sqlRowStreams.add(sqlRowStream);
            allStreams.put(sqlConnection, sqlRowStreams);
        } else {
            sqlRowStreams.add(sqlRowStream);
        }
    }
}