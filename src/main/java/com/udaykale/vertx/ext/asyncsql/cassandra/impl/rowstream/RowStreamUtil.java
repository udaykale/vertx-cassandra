package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.core.Handler;

/**
 * @author uday
 */
final class RowStreamUtil {

    static void handleIllegalStateException(RowStreamInfo rowStreamInfo, String message) {
        IllegalStateException e = new IllegalStateException(message);

        if (rowStreamInfo.getExceptionHandler().isPresent()) {
            Handler<Throwable> exceptionHandler = rowStreamInfo.getExceptionHandler().get();
            exceptionHandler.handle(e);
        } else {
            throw e;
        }
    }
}
