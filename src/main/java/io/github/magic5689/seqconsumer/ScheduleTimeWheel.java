package io.github.magic5689.seqconsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * A hashed-wheel timer for lightweight delayed task scheduling.
 * Uses a lock-free MPSC queue per slot via CAS.
 */
public class ScheduleTimeWheel {
    private long wheelPer;
    private MpscTaskQueue[] wheel;
    private final AtomicBoolean flag = new AtomicBoolean(false);
    private final WheelThread wheelThread;
    private final ExecutorService executor;

    public ScheduleTimeWheel() {
        this.wheelThread = new WheelThread();
        this.wheelThread.setDaemon(false);
        this.executor = Executors.newFixedThreadPool(6);
    }

    public ScheduleTimeWheel(ExecutorService executors) {
        this.wheelThread = new WheelThread();
        this.wheelThread.setDaemon(false);
        this.executor = executors;
    }

    /**
     * Configure the time wheel.
     *
     * @param wheelPer  tick interval duration
     * @param timeUnit  time unit of wheelPer
     * @param slotCount total number of slots
     */
    public void setWheel(long wheelPer, TimeUnit timeUnit, int slotCount) {
        if (slotCount <= 0) {
            throw new IllegalArgumentException("slotCount must be > 0: " + slotCount);
        }
        this.wheelPer = timeUnit.toMillis(wheelPer);
        if (this.wheelPer % 100 != 0) {
            throw new IllegalArgumentException("wheelPer millis must be a multiple of 100");
        }
        wheel = new MpscTaskQueue[slotCount];
        for (int i = 0; i < slotCount; i++) {
            wheel[i] = new MpscTaskQueue();
        }
    }

    public void stop() {
        if (flag.compareAndSet(true, false)) {
            LockSupport.unpark(wheelThread);
        }
        System.out.println("TimeWheel stopped");
    }

    /**
     * Add a delayed task to the wheel.
     *
     * @param runnable the task
     * @param delay    delay duration
     * @param timeUnit time unit of delay
     * @return a cancellable DelayTask handle
     */
    public DelayTask addDelaTask(Runnable runnable, long delay, TimeUnit timeUnit) {
        if (runnable == null) {
            throw new NullPointerException("runnable must not be null");
        }
        if (delay < 0) {
            throw new IllegalArgumentException("delay must be >= 0: " + delay);
        }
        if (wheel == null) {
            throw new IllegalStateException("TimeWheel not initialized, call setWheel first");
        }
        start();

        long delayMs = timeUnit.toMillis(delay);
        DelayTask task = new DelayTask(runnable, delayMs);
        int needSlot = (int) (delayMs / this.wheelPer);
        int taskLocation = (wheelThread.index + needSlot) % this.wheel.length;
        MpscTaskQueue taskQueue = wheel[taskLocation];
        taskQueue.push(task);
        return task;
    }

    private void start() {
        if (flag.compareAndSet(false, true)) {
            wheelThread.start();
        }
    }

    private class WheelThread extends Thread {
        long step = 0;
        long baseTime = System.currentTimeMillis();
        int index = 0;

        @Override
        public void run() {
            while (flag.get()) {
                long currentTime = System.currentTimeMillis();
                long nextTime = baseTime + (step + 1) * wheelPer;
                long waitTime = nextTime - currentTime;
                index = Math.toIntExact(step % wheel.length);
                while (waitTime > 0) {
                    LockSupport.parkUntil(currentTime + waitTime);
                    currentTime = System.currentTimeMillis();
                    waitTime = nextTime - currentTime;
                    if (!flag.get()) {
                        shutdownExecutor();
                        return;
                    }
                }
                MpscTaskQueue taskQueue = wheel[index];
                List<Runnable> runnables = taskQueue.removeAndReturnShouldRun(nextTime);
                runnables.forEach(executor::execute);
                step++;
                if (step % wheel.length == 0) {
                    step = 0;
                    baseTime = nextTime;
                }
            }
            shutdownExecutor();
        }

        private void shutdownExecutor() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        throw new RuntimeException("TimeWheel executor failed to terminate");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                throw new RuntimeException("TimeWheel executor interrupted during shutdown", e);
            }
            System.out.println("TimeWheel executor shut down");
        }
    }

    public static class DelayTask {
        final Runnable runnable;
        long deadLine;
        DelayTask next;

        public DelayTask(Runnable runnable, long delayMs) {
            this.runnable = runnable;
            this.deadLine = System.currentTimeMillis() + delayMs;
        }

        public void cancel() {
            this.deadLine = -1;
        }
    }

    private static class MpscTaskQueue {
        final AtomicReference<DelayTask> head = new AtomicReference<>(null);

        public List<Runnable> removeAndReturnShouldRun(long triggerTime) {
            List<Runnable> list = new ArrayList<>();
            DelayTask current = head.get();
            DelayTask pre = null;
            while (current != null) {
                if (current.deadLine > triggerTime) {
                    pre = current;
                    current = current.next;
                    continue;
                }
                DelayTask next = current.next;
                if (pre != null) {
                    pre.next = next;
                    if (current.deadLine != -1) {
                        list.add(current.runnable);
                    }
                    current.next = null;
                    current = next;
                }
                if (head.compareAndSet(current, next)) {
                    if (current != null) {
                        if (current.deadLine != -1) {
                            list.add(current.runnable);
                        }
                        current.next = null;
                        current = next;
                        continue;
                    }
                }
                current = head.get();
            }
            return list;
        }

        public void push(DelayTask delayTask) {
            while (true) {
                DelayTask oldHead = head.get();
                delayTask.next = oldHead;
                if (head.compareAndSet(oldHead, delayTask)) {
                    return;
                }
            }
        }
    }
}
