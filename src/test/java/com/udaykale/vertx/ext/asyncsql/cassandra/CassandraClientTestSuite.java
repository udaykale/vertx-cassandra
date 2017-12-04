package com.udaykale.vertx.ext.asyncsql.cassandra;

import com.udaykale.vertx.ext.asyncsql.cassandra.client.CloseTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.CreationTest;
import com.udaykale.vertx.ext.asyncsql.cassandra.client.GetConnectionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

/**
 * @author uday
 */
@RunWith(Suite.class)
@SuiteClasses({
        CloseTest.class,
        CreationTest.class,
        GetConnectionTest.class,
})
public class CassandraClientTestSuite {
}
