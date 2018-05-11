package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.core.Handler;

/**
 * @author uday
 */
final class RowStreamUtil {

    static void handleIllegalStateException(RowStreamInfoWrapper rowStreamInfoWrapper, String message) {
        IllegalStateException e = new IllegalStateException(message);

        if (rowStreamInfoWrapper.getExceptionHandler().isPresent()) {
            Handler<Throwable> exceptionHandler = rowStreamInfoWrapper.getExceptionHandler().get();
            exceptionHandler.handle(e);
        } else {
            throw e;
        }
    }
}
