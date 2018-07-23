package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientTestSuite;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.connection.CassandraConnectionTestSuite;
import com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream.CassandraRowStreamTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        CassandraClientTestSuite.class,
        CassandraRowStreamTestSuite.class,
        CassandraConnectionTestSuite.class
})
public class VertxCassandraTestSuite {
}
