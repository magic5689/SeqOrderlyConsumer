package io.github.magic5689.seqconsumer;

import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A session-based sequential consumer that guarantees tasks within the same
 * session execute in strict seqNo order, with per-session timeout fallback
 * backed by a {@link ScheduleTimeWheel}.
 *
 * <p>Typical use cases: ordered message processing, trading system callbacks,
 * IoT command sequencing — any scenario where messages sharing a session key
 * must be consumed in arrival order but you don't want to block forever when
 * a gap appears.
 *
 * <pre>{@code
 * SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();
 * consumer.submitTask(() -> process(msg1), sessionId, 1, false, 30);
 * consumer.submitTask(() -> process(msg2), sessionId, 2, false, 30);
 * consumer.submitTask(() -> process(msg3), sessionId, 3, true,  30);
 * // msg1, msg2, msg3 execute in order; if msg2 never arrives,
 * // the timeout fires after 30s and msg3 proceeds.
 * }</pre>
 */
public class SeqOrderlyConsumer {
    private final ConcurrentHashMap<Integer, PriorityBlockingQueue<ConsumerTask>> queueMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> seq = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, TaskConsumerTime> timeOutMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ScheduleTimeWheel.DelayTask> delayTaskMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AtomicBoolean> consumerGuard = new ConcurrentHashMap<>();
    private final ScheduleTimeWheel wheel;
    private final ExecutorService executor;

    public SeqOrderlyConsumer() {
        wheel = new ScheduleTimeWheel();
        wheel.setWheel(1, TimeUnit.SECONDS, 4);
        executor = Executors.newCachedThreadPool();
    }

    public SeqOrderlyConsumer(ScheduleTimeWheel wheel) {
        this.wheel = wheel;
        this.executor = Executors.newCachedThreadPool();
    }

    public SeqOrderlyConsumer(ScheduleTimeWheel wheel, ExecutorService executorService) {
        this.wheel = wheel;
        this.executor = executorService;
    }

    public void clear(Integer sessionId) {
        queueMap.remove(sessionId);
        seq.remove(sessionId);
        timeOutMap.remove(sessionId);
        consumerGuard.remove(sessionId);
        ScheduleTimeWheel.DelayTask dt = delayTaskMap.remove(sessionId);
        if (dt != null) {
            dt.cancel();
        }
    }

    /**
     * Submit a task for ordered execution within a session.
     *
     * @param runnable       the task to execute
     * @param sessionId      session key — tasks sharing the same key are ordered
     * @param seqNo          sequence number, must start from 1 and be consecutive
     * @param lastTask       whether this is the final task of the session
     * @param waitTimeSecond timeout in seconds; if the expected next seqNo
     *                       doesn't arrive within this window the task is force-executed
     */
    public void submitTask(Runnable runnable, Integer sessionId, int seqNo,
                           boolean lastTask, long waitTimeSecond) {
        if (seqNo <= 0) {
            throw new IllegalArgumentException("seqNo must be > 0");
        }
        seq.putIfAbsent(sessionId, 1);

        PriorityBlockingQueue<ConsumerTask> queue = queueMap.compute(sessionId, (k, v) -> {
            if (v == null) {
                v = new PriorityBlockingQueue<>(10, Comparator.comparingInt(ConsumerTask::getSeqNo));
            }
            ConsumerTask task = new ConsumerTask();
            task.setRunnable(runnable);
            task.setSessionId(sessionId);
            task.setSeqNo(seqNo);
            task.setLastTask(lastTask);
            v.add(task);

            ScheduleTimeWheel.DelayTask delayTask1 = delayTaskMap.get(sessionId);
            if (delayTask1 != null) {
                delayTask1.cancel();
            }

            TaskConsumerTime taskConsumerTime = new TaskConsumerTime();
            taskConsumerTime.setWaitTimeSecond(waitTimeSecond);
            taskConsumerTime.setLastConsumerTime(-1);
            timeOutMap.putIfAbsent(sessionId, taskConsumerTime);

            ScheduleTimeWheel.DelayTask delayTask = wheel.addDelaTask(() -> {
                PriorityBlockingQueue<ConsumerTask> tasks = queueMap.get(sessionId);
                if (tasks == null) return;
                ConsumerTask consumerTask = tasks.poll();
                TaskConsumerTime consumerTime = timeOutMap.get(sessionId);
                if (consumerTime == null || consumerTime.getLastConsumerTime() == -1) return;
                if (consumerTask == null) return;
                long currentTime = System.currentTimeMillis();
                long lastConsumerTime = consumerTime.getLastConsumerTime();
                if (currentTime > lastConsumerTime) {
                    System.out.printf("Task session=%s seq=%s timed out%n",
                            consumerTask.getSessionId(), consumerTask.getSeqNo());
                    seq.computeIfPresent(consumerTask.getSessionId(), (k1, v1) -> v1 + 1);
                    consumerTask.runnable.run();
                }
            }, waitTimeSecond, TimeUnit.SECONDS);

            delayTaskMap.put(sessionId, delayTask);
            return v;
        });

        // Ensure only one consumer thread runs per session
        AtomicBoolean guard = consumerGuard.computeIfAbsent(sessionId, k -> new AtomicBoolean(false));
        if (guard.compareAndSet(false, true)) {
            executor.submit(() -> {
                try {
                    startConsumer(queue);
                } finally {
                    guard.set(false);
                }
            });
        }
    }

    private void startConsumer(PriorityBlockingQueue<ConsumerTask> queue) {
        while (true) {
            ConsumerTask task = queue.peek();
            if (task == null) break;
            synchronized (task) {
                Integer sessionId = task.getSessionId();
                Integer currentSeq = seq.get(sessionId);
                if (currentSeq == null) {
                    queue.poll();
                    continue;
                }
                if (seq.get(task.getSessionId()).equals(task.getSeqNo())) {
                    queue.poll();
                    task.runnable.run();
                    if (task.isLastTask()) {
                        clear(task.getSessionId());
                        return;
                    }
                    timeOutMap.get(task.getSessionId()).setLastConsumerTime(System.currentTimeMillis());
                    seq.computeIfPresent(task.getSessionId(), (k, v) -> v + 1);
                }
                if (queue.isEmpty()) return;
            }
        }
    }

    private static class TaskConsumerTime {
        private volatile long lastConsumerTime;
        private volatile long waitTimeSecond;

        public long getLastConsumerTime() {
            return lastConsumerTime;
        }

        public void setLastConsumerTime(long lastConsumerTime) {
            this.lastConsumerTime = lastConsumerTime;
        }

        public long getWaitTimeSecond() {
            return waitTimeSecond;
        }

        public void setWaitTimeSecond(long waitTimeSecond) {
            this.waitTimeSecond = waitTimeSecond;
        }
    }

    private static class ConsumerTask {
        private Runnable runnable;
        private Integer sessionId;
        private int seqNo;
        private boolean lastTask;

        public boolean isLastTask() {
            return lastTask;
        }

        public void setLastTask(boolean lastTask) {
            this.lastTask = lastTask;
        }

        public int getSeqNo() {
            return seqNo;
        }

        public void setSeqNo(int seqNo) {
            this.seqNo = seqNo;
        }

        public Runnable getRunnable() {
            return runnable;
        }

        public void setRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        public Integer getSessionId() {
            return sessionId;
        }

        public void setSessionId(Integer sessionId) {
            this.sessionId = sessionId;
        }
    }
}
