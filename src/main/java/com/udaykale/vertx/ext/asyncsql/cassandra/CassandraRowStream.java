package com.udaykale.vertx.ext.asyncsql.cassandra;

import io.vertx.ext.sql.SQLRowStream;

public interface CassandraRowStream extends SQLRowStream, Comparable<CassandraRowStream> {

    int getStreamId();

    @Override
    default int compareTo(CassandraRowStream that) {
        return this.getStreamId() - that.getStreamId();
    }
}
