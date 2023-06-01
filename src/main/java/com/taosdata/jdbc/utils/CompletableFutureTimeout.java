package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;

import java.util.concurrent.*;
import java.util.function.Function;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_QUERY_TIMEOUT;

public class CompletableFutureTimeout {
    private CompletableFutureTimeout() {
    }

    public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit) {
        final CompletableFuture<T> timeoutFuture = timeoutAfter(timeout, unit);
        return future.applyToEither(timeoutFuture, Function.identity());
    }

    // return the default value when exception
    public static <T> CompletableFuture<T> orTimeout(T t, CompletableFuture<T> future, long timeout, TimeUnit unit) {
        final CompletableFuture<T> timeoutFuture = timeoutAfter(timeout, unit);
        return future.applyToEither(timeoutFuture, Function.identity()).exceptionally(throwable -> t);
    }

    private static <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit) {
        CompletableFuture<T> result = new CompletableFuture<>();
        Delayer.delayer.schedule(
                () -> result.completeExceptionally(TSDBError.createTimeoutException(ERROR_QUERY_TIMEOUT,
                        String.format("failed to complete the task within the specified time : %d,%s", timeout, unit))), timeout, unit)
        ;
        return result;
    }

    /**
     * Singleton delay scheduler, used only for starting and * cancelling tasks.
     */
    static final class Delayer {
        private Delayer() {
        }

        @SuppressWarnings("all")
        static ScheduledFuture<?> delay(Runnable command, long delay,
                                        TimeUnit unit) {
            return delayer.schedule(command, delay, unit);
        }

        static final class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("DelayScheduler-");
                return t;
            }
        }

        static final ScheduledThreadPoolExecutor delayer;

        static {
            delayer = new ScheduledThreadPoolExecutor(
                    1, new DaemonThreadFactory());
            delayer.setRemoveOnCancelPolicy(true);
        }
    }
}