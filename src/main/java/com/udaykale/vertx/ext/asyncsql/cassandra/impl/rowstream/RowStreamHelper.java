package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLRowStream;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author uday
 */
final class RowStreamHelper {

    static RowStreamInfo rowStreamInfo(WorkerExecutor workerExecutor,
                                       Set<SQLRowStream> allRowStreams,
                                       Function<Row, JsonArray> rowMapper,
                                       CassandraRowStreamImpl cassandraRowStream,
                                       ResultSet resultSet) {
        RowStreamState currentState = IsPausedRowStreamState.instance(cassandraRowStream);
        RowStreamInfo rowStreamInfo = new RowStreamInfo(currentState);

        rowStreamInfo.setWorkerExecutor(workerExecutor);
        rowStreamInfo.setAllRowStreams(allRowStreams);
        rowStreamInfo.setResultSet(resultSet);
        rowStreamInfo.setRowMapper(rowStreamMapper(resultSet, rowMapper));

        return rowStreamInfo;
    }

    static Function<Row, JsonArray> rowStreamMapper(ResultSet resultSet, Function<Row, JsonArray> rowMapper) {
        int numColumns = resultSet.getColumnDefinitions().size();
        return Optional.ofNullable(rowMapper).orElse(defaultRowMapper(numColumns));
    }

    static Function<Row, JsonArray> defaultRowMapper(int numColumns) {
        return row -> {
            JsonArray jsonArray = new JsonArray();

            for (int i = 0; i < numColumns; i++) {
                Object value = row.getObject(i);
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
