package com.udaykale.vertx.ext.asyncsql.cassandra.impl.rowstream;

import io.vertx.core.Handler;

final class RowStreamUtil {

    static void handleIllegalStateException(RowStreamStateWrapper rowStreamStateWrapper, String message,
                                            Handler<Throwable> exceptionHandler) {
        IllegalStateException e = new IllegalStateException(message);

        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
        } else {
            throw e;
        }
    }
}
