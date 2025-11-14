package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.*;
import java.util.function.Function;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_QUERY_TIMEOUT;

public class CompletableFutureTimeout {
    private CompletableFutureTimeout() {
    }

    public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit, String msg) {
        final CompletableFuture<T> timeoutFuture = timeoutAfter(timeout, unit, msg);
        future.whenCompleteAsync((result, throwable) -> {
            if (future.isDone() && !timeoutFuture.isDone()) {
                timeoutFuture.cancel(false);
            }
        });
        return future.applyToEither(timeoutFuture, Function.identity());
    }

    private static <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit, String msg) {
        if (TimeUnit.MILLISECONDS.convert(timeout, unit)  <= TSDBConstants.DEFAULT_MESSAGE_WAIT_TIMEOUT) {
            // 短超时：使用Netty HashedWheelTimer
            return handleShortTimeout(timeout, unit, msg);
        } else {
            // 长超时：使用JDK ScheduledThreadPoolExecutor
            return handleLongTimeout(timeout, unit, msg);
        }
    }

    /**
     * 处理短超时任务（≤60秒）：使用Netty定时器
     */
    private static <T> CompletableFuture<T>  handleShortTimeout(
            long timeout,
            TimeUnit unit,
            String msg) {
        CompletableFuture<T> result = new CompletableFuture<>();

        TimerTask task = (Timeout t) -> {
            if (!result.isDone()) {
                Utils.getEventLoopGroup().execute(() -> {
                    result.completeExceptionally(TSDBError.createTimeoutException(ERROR_QUERY_TIMEOUT,
                            String.format("failed to complete the task:%s within the specified time : %d,%s", msg, timeout, unit))
                    );
                });
            }
        };

        io.netty.util.Timeout nettyTimeout = NETTY_TIMER.newTimeout(task, timeout, unit);

        result.handle((res, ex) -> {
            if (!nettyTimeout.isCancelled()) {
                nettyTimeout.cancel();
            }
            return null;
        });

        return  result;
    }

    /**
     * 处理长超时任务（>60秒）：使用JDK调度器
     */
    private static <T> CompletableFuture<T>  handleLongTimeout(
            long timeout,
            TimeUnit unit,
            String msg) {

        CompletableFuture<T> result = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = Delayer.delayer.schedule(
                () -> result.completeExceptionally(TSDBError.createTimeoutException(ERROR_QUERY_TIMEOUT,
                        String.format("failed to complete the task:%s within the specified time : %d,%s", msg, timeout, unit)))
                , timeout, unit);

        // Use handle to ensure the scheduled task is cancelled when the CompletableFuture completes normally or exceptionally
        result.handle((res, ex) -> {
            scheduledFuture.cancel(false);
            return null;
        });

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
                t.setName("taos-jdbc-long-timer-");
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

    private static final Timer NETTY_TIMER = new HashedWheelTimer(
            new DefaultThreadFactory("taos-jdbc-short-timer"),
            50, TimeUnit.MILLISECONDS,
            1024 // slot（2^10，覆盖 1024 * 50ms = 51.2, 60s task will handled in 2 loops）
    );
}