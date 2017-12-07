package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.udaykale.vertx.ext.asyncsql.cassandra.impl.client.CassandraClientTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

/**
 * @author uday
 */
@RunWith(Suite.class)
@SuiteClasses({
        CassandraClientTestSuite.class
})
public class VertxCassandraTestSuite {
}
