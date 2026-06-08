package com.taosdata.jdbc.ws;

import org.junit.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class WSEWDispatchStrategyTest {

    @Test
    public void dispatchSerializationTask_returnsBeforeTaskCompletesOnBaseClass() throws Exception {
        Method method = AbstractWSEWPreparedStatement.class
                .getDeclaredMethod("dispatchSerializationTask", RecursiveAction.class);
        method.setAccessible(true);
        WSEWColumnPreparedStatement stmt = allocateWithoutConstructor(WSEWColumnPreparedStatement.class);

        assertDispatchReturnsBeforeTaskCompletes("base", action -> {
            try {
                method.invoke(stmt, action);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void columnarWSEW_dispatchSerializationTask_returnsBeforeTaskCompletes() throws Exception {
        WSEWColumnPreparedStatement stmt = allocateWithoutConstructor(WSEWColumnPreparedStatement.class);

        assertDispatchReturnsBeforeTaskCompletes("columnar",
                action -> invokeDeclaredDispatch(WSEWColumnPreparedStatement.class, stmt, action));
    }

    @Test
    public void legacyWSEW_dispatchSerializationTask_returnsBeforeTaskCompletes() throws Exception {
        WSEWPreparedStatement stmt = allocateWithoutConstructor(WSEWPreparedStatement.class);

        assertDispatchReturnsBeforeTaskCompletes("legacy",
                action -> invokeDeclaredDispatch(WSEWPreparedStatement.class, stmt, action));
    }

    private static synchronized void assertDispatchReturnsBeforeTaskCompletes(
            String label, DispatchCall dispatchCall) throws Exception {
        BlockingAction action = new BlockingAction();
        AtomicReference<Thread> callerThread = new AtomicReference<Thread>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(() -> {
                callerThread.set(Thread.currentThread());
                dispatchCall.dispatch(action);
            });

            assertTrue(label + " dispatch should start the task asynchronously",
                    action.entered.await(1, TimeUnit.SECONDS));
            future.get(1, TimeUnit.SECONDS);

            assertNotSame(label + " dispatch should not serialize on the caller thread",
                    callerThread.get(), action.executingThread.get());

            action.release.countDown();
            assertTrue(label + " task should finish after release", action.completed.await(1, TimeUnit.SECONDS));
        } finally {
            action.release.countDown();
            executor.shutdownNow();
        }
    }

    private static void invokeDeclaredDispatch(Class<?> owner, Object target, RecursiveAction action) {
        try {
            Method method = findDispatchMethod(owner);
            method.setAccessible(true);
            method.invoke(target, action);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Method findDispatchMethod(Class<?> owner) throws NoSuchMethodException {
        Class<?> current = owner;
        while (current != null) {
            try {
                return current.getDeclaredMethod("dispatchSerializationTask", RecursiveAction.class);
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException("dispatchSerializationTask");
    }

    @FunctionalInterface
    private interface DispatchCall {
        void dispatch(RecursiveAction action);
    }

    @SuppressWarnings("unchecked")
    private static <T> T allocateWithoutConstructor(Class<T> type) throws Exception {
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        Unsafe unsafe = (Unsafe) field.get(null);
        return (T) unsafe.allocateInstance(type);
    }

    private static final class BlockingAction extends RecursiveAction {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();

        @Override
        protected void compute() {
            executingThread.set(Thread.currentThread());
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                completed.countDown();
            }
        }
    }
}
