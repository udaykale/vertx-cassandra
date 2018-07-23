package com.udaykale.vertx.ext.asyncsql.cassandra.impl.client;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        EqualsAndHashCodeTest.class,
        ClientConnectionTest.class,
        CreationArgumentTest.class,
        CreationTest.class
})
public class CassandraClientTestSuite {
}
