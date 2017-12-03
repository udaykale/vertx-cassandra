package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * @author uday
 */
final class RowStreamHelper {

    static List<String> columns(ResultSet resultSet) {
        return resultSet.getColumnDefinitions()
                .asList().stream()
                .map(ColumnDefinitions.Definition::getName)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
    }

    static RowStreamStateWrapper stateWrapper(WorkerExecutor workerExecutor,
                                              Set<SQLRowStream> allRowStreams,
                                              Function<Row, JsonArray> rowMapper,
                                              CassandraRowStreamImpl cassandraRowStream,
                                              List<String> columns, ResultSet resultSet) {
        RowStreamState currentState = IsPausedRowStreamState.instance(cassandraRowStream);
        RowStreamStateWrapper stateWrapper = new RowStreamStateWrapper(currentState);

        Function<Row, JsonArray> defaultRowMapper = defaultRowMapper(columns);
        Function<Row, JsonArray> finalRowMapper = Optional.ofNullable(rowMapper).orElse(defaultRowMapper);
        stateWrapper.setRowMapper(finalRowMapper);

        stateWrapper.setWorkerExecutor(workerExecutor);
        stateWrapper.setAllRowStreams(allRowStreams);
        stateWrapper.setResultSet(resultSet);

        return stateWrapper;
    }

    private static Function<Row, JsonArray> defaultRowMapper(List<String> columns) {
        return row -> {
            JsonArray jsonArray = new JsonArray();

            for (String column : columns) {
                Object value = row.getObject(column);
                if (value instanceof String) {
                    jsonArray.add((String) value);
                } else if (value instanceof Integer) {
                    jsonArray.add((Integer) value);
                } else if (value instanceof Long) {
                    jsonArray.add((Long) value);
                } else if (value instanceof Float) {
                    jsonArray.add((Float) value);
                } else if (value instanceof Boolean) {
                    jsonArray.add((Boolean) value);
                } else {
                    jsonArray.add(value);
                }
            }

            return jsonArray;
        };
    }
}
