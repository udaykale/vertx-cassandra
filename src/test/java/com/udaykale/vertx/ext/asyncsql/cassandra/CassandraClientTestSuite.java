package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.udaykale.vertx.ext.asyncsql.cassandra.client.CreationArgumentTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.CreationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

/**
 * @author uday
 */
@RunWith(Suite.class)
@SuiteClasses({
        CreationArgumentTest.class,
        CreationTest.class
})
public class CassandraClientTestSuite {
}
