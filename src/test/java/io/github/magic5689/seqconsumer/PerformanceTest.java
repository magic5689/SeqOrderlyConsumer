package io.github.magic5689.seqconsumer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能测试套件，覆盖吞吐、延迟、多 session 并发、时间轮、stop 关闭。
 * 零外部依赖，直接运行 main 即可。
 */
public class PerformanceTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURE_ITERATIONS = 5;

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("  SeqOrderlyConsumer 性能测试");
        System.out.println("========================================\n");

        testSingleSessionThroughput();
        testMultiSessionThroughput();
        testOutOfOrderGapRecovery();
        testTimeWheelAddDelay();
        testStopShutdownLatency();
        testHighConcurrencyStress();

        System.out.println("\n所有测试完成。");
    }

    // ── 1. 单 session 顺序吞吐 ──────────────────────────────────

    private static void testSingleSessionThroughput() throws Exception {
        System.out.println("── 1. 单 Session 顺序吞吐 ──");

        for (int total : new int[]{1000, 10000, 100000}) {
            long[] latencies = new long[total];
            AtomicLong counter = new AtomicLong(0);

            SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();
            long start = System.nanoTime();

            for (int i = 1; i <= total; i++) {
                final int idx = i - 1;
                final long before = System.nanoTime();
                consumer.submitTask(() -> {
                    latencies[idx] = System.nanoTime() - before;
                    counter.incrementAndGet();
                }, "s1", i, i == total, 30, true);
            }

            while (counter.get() < total) {
                Thread.onSpinWait();
            }
            long elapsed = System.nanoTime() - start;

            long sumLatency = 0;
            long maxLatency = 0;
            for (long l : latencies) {
                sumLatency += l;
                if (l > maxLatency) maxLatency = l;
            }

            System.out.printf("  任务数=%-6d  总耗时=%5dms  吞吐=%,d/s  avg=%.1fµs  max=%.1fµs%n",
                    total,
                    TimeUnit.NANOSECONDS.toMillis(elapsed),
                    total * 1_000_000_000L / elapsed,
                    sumLatency / (double) total / 1000.0,
                    maxLatency / 1000.0);

            consumer.stop();
        }
    }

    // ── 2. 多 session 并发吞吐 ──────────────────────────────────

    private static void testMultiSessionThroughput() throws Exception {
        System.out.println("\n── 2. 多 Session 并发吞吐 ──");

        int sessionCount = 50;
        int tasksPerSession = 2000;
        int totalTasks = sessionCount * tasksPerSession;
        AtomicLong completed = new AtomicLong(0);

        SeqOrderlyConsumer consumer = new SeqOrderlyConsumer(
                new ThreadPoolExecutor(50, 50, 60L, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>()));

        long start = System.nanoTime();

        CountDownLatch latch = new CountDownLatch(1);
        for (int s = 0; s < sessionCount; s++) {
            final String sid = "multi-" + s;
            new Thread(() -> {
                try { latch.await(); } catch (InterruptedException e) { return; }
                for (int i = 1; i <= tasksPerSession; i++) {
                    consumer.submitTask(completed::incrementAndGet,
                            sid, i, i == tasksPerSession, 30, true);
                }
            }).start();
        }

        latch.countDown();

        while (completed.get() < totalTasks) {
            Thread.onSpinWait();
        }
        long elapsed = System.nanoTime() - start;

        System.out.printf("  session=%d  每session任务=%d  总耗时=%dms  吞吐=%,d/s%n",
                sessionCount, tasksPerSession,
                TimeUnit.NANOSECONDS.toMillis(elapsed),
                totalTasks * 1_000_000_000L / elapsed);

        consumer.stop();
    }

    // ── 3. 乱序到达排序 ────────────────────────────────────────

    private static void testOutOfOrderGapRecovery() throws Exception {
        System.out.println("\n── 3. 乱序到达排序 ──");

        int rounds = 50;
        long totalLatency = 0;

        for (int r = 0; r < rounds; r++) {
            SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();
            AtomicLong last = new AtomicLong(0);
            long before = System.nanoTime();

            // 模拟网络乱序：seq=1,3 先到，seq=2 延迟 5ms 后到
            // 消费者按 1→2→3 顺序执行，task3 为 lastTask
            consumer.submitTask(() -> last.set(1), "gap", 1, false, 30, true);
            consumer.submitTask(() -> last.set(3), "gap", 3, true, 30, true);
            Thread.sleep(5);
            consumer.submitTask(() -> last.set(2), "gap", 2, false, 30, true);

            // 等待全部消费完成（3 为 lastTask 会触发自动清理）
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (last.get() != 3 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            if (last.get() != 3) {
                System.out.printf("  [WARN] 轮次 %d 未完成 (last=%d)%n", r, last.get());
            }
            totalLatency += System.nanoTime() - before;
            consumer.stop();
        }

        System.out.printf("  轮次=%d  avg端到端延迟=%.1fµs%n",
                rounds, totalLatency / (double) rounds / 1000.0);

        // 3b. 超时缺口推进：缺 seq=2，验证超时后跳过
        System.out.println("\n── 3b. 超时缺口推进 ──");

        SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();
        AtomicLong executed = new AtomicLong(0);

        consumer.submitTask(() -> executed.set(1), "timeout", 1, false, 1, true);
        consumer.submitTask(() -> executed.set(3), "timeout", 3, true, 1, true);
        // 不提交 seq=2，等待超时推进

        long start = System.nanoTime();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (executed.get() != 3 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        long elapsed = System.nanoTime() - start;

        if (executed.get() == 3) {
            System.out.printf("  超时推进成功  seq=2 被跳过  耗时=%.0fms%n",
                    elapsed / 1_000_000.0);
        } else {
            System.out.printf("  [FAIL] 超时推进失败  executed=%d%n", executed.get());
        }

        consumer.stop();
    }

    // ── 4. 时间轮 addDelayTask 性能 ────────────────────────────

    private static void testTimeWheelAddDelay() throws Exception {
        System.out.println("\n── 4. 时间轮 addDelayTask 性能 ──");

        ScheduleTimeWheel wheel = new ScheduleTimeWheel();
        wheel.setWheel(100, TimeUnit.MILLISECONDS, 60);

        int count = 50000;

        // warmup
        for (int i = 0; i < 5000; i++) {
            wheel.addDelayTask(() -> {}, 10, TimeUnit.SECONDS);
        }
        Thread.sleep(500);

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            wheel.addDelayTask(() -> {}, 10, TimeUnit.SECONDS);
        }
        long elapsed = System.nanoTime() - start;

        System.out.printf("  添加 %d 个延时任务  总耗时=%dms  avg=%.1fµs  吞吐=%,d/s%n",
                count,
                TimeUnit.NANOSECONDS.toMillis(elapsed),
                elapsed / (double) count / 1000.0,
                count * 1_000_000_000L / elapsed);

        wheel.stop();
    }

    // ── 5. stop() 关闭延迟 ─────────────────────────────────────

    private static void testStopShutdownLatency() throws Exception {
        System.out.println("\n── 5. stop() 关闭延迟 ──");

        int sessions = 100;
        long totalLatency = 0;

        for (int r = 0; r < 10; r++) {
            SeqOrderlyConsumer consumer = new SeqOrderlyConsumer(
                    new ThreadPoolExecutor(sessions, sessions,
                            60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>()));

            // 提交一些任务创建 consumer 线程
            for (int s = 0; s < sessions; s++) {
                final String sid = "stop-" + s;
                consumer.submitTask(() -> {}, sid, 1, true, 5, true);
            }
            Thread.sleep(100); // 等任务执行完

            long before = System.nanoTime();
            consumer.stop();
            long elapsed = System.nanoTime() - before;
            totalLatency += elapsed;
        }

        System.out.printf("  关闭 %d 个 session  平均延迟=%.1fms%n",
                sessions, totalLatency / 10.0 / 1_000_000.0);
    }

    // ── 6. 高并发压力测试 ──────────────────────────────────────

    private static void testHighConcurrencyStress() throws Exception {
        System.out.println("\n── 6. 高并发压力测试 ──");

        int sessionCount = 200;
        int tasksPerSession = 500;
        int total = sessionCount * tasksPerSession;
        AtomicLong completed = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        ExecutorService producerPool = Executors.newFixedThreadPool(20);
        SeqOrderlyConsumer consumer = new SeqOrderlyConsumer(
                new ThreadPoolExecutor(200, 200,
                        60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                        new ThreadPoolExecutor.CallerRunsPolicy()));

        long start = System.nanoTime();

        CountDownLatch latch = new CountDownLatch(1);
        for (int s = 0; s < sessionCount; s++) {
            final String sid = "stress-" + s;
            producerPool.submit(() -> {
                try { latch.await(); } catch (InterruptedException e) { return; }
                for (int i = 1; i <= tasksPerSession; i++) {
                    try {
                        consumer.submitTask(completed::incrementAndGet,
                                sid, i, i == tasksPerSession, 30, true);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
        }

        latch.countDown();
        producerPool.shutdown();
        producerPool.awaitTermination(1, TimeUnit.MINUTES);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (completed.get() < total && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }

        long elapsed = System.nanoTime() - start;

        System.out.printf("  session=%d  每session=%d  完成=%d/%d  错误=%d%n",
                sessionCount, tasksPerSession, completed.get(), total, errors.get());
        System.out.printf("  总耗时=%dms  吞吐=%,d/s%n",
                TimeUnit.NANOSECONDS.toMillis(elapsed),
                completed.get() * 1_000_000_000L / Math.max(elapsed, 1));

        consumer.stop();
    }
}
