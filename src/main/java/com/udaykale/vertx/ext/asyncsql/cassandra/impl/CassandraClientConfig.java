package com.udaykale.vertx.ext.asyncsql.cassandra.impl;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

final class CassandraClientConfig implements Shareable {

    private List<String> contactPoints;
    private String username;
    private String password;
    private String keyspace;

    private CassandraClientConfig() {
    }

    static CassandraClientConfig loadCassandraClientConfig(JsonObject config) {
        CassandraClientConfig cassandraClientConfig = new CassandraClientConfig();

        String hosts = config.getString("cassandra.contact_points", "");
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("Contact points instance cassandra not specified in config");
        }
        cassandraClientConfig.contactPoints = Arrays.stream(hosts.split(","))
                .collect(Collectors.collectingAndThen(toList(), Collections::unmodifiableList));
        cassandraClientConfig.username = config.getString("cassandra.username", "");
        cassandraClientConfig.password = config.getString("cassandra.password", "");
        cassandraClientConfig.keyspace = config.getString("cassandra.keyspace", "");

        return cassandraClientConfig;
    }

    public List<String> getContactPoints() {
        return contactPoints;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getKeyspace() {
        return keyspace;
    }
}
