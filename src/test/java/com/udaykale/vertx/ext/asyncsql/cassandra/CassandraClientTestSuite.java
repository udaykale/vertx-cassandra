package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.udaykale.vertx.ext.asyncsql.cassandra.client.ClientConnectionTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.CreationArgumentTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.CreationTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.EqualsAndHashCodeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

/**
 * @author uday
 */
@RunWith(Suite.class)
@SuiteClasses({
        EqualsAndHashCodeTest.class,
        ClientConnectionTest.class,
        CreationArgumentTest.class,
        CreationTest.class
})
public class CassandraClientTestSuite {
}
