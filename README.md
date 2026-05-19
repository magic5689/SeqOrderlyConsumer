# SeqOrderlyConsumer

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)]()

**SeqOrderlyConsumer** 是一个零依赖、轻量级的 Java 库，用于**按会话（session）严格顺序消费任务**，内置超时兜底机制，底层基于自研的**哈希时间轮**驱动超时调度。

---

## 目录

- [适用场景](#适用场景)
- [项目规模适用性](#项目规模适用性)
- [核心能力](#核心能力)
- [快速开始](#快速开始)
- [API 参考](#api-参考)
- [架构设计](#架构设计)
- [生产环境评估](#生产环境评估)
- [最佳实践](#最佳实践)
- [Maven 依赖](#maven-依赖)
- [License](#license)

---

## 适用场景

### 什么情况下你应该使用 SeqOrderlyConsumer？

| 场景 | 说明 |
|---|---|
| **IoT 设备指令下发** | 同一设备（sessionId=设备ID）的控制指令必须按序执行，乱序可能导致设备状态异常 |
| **交易/支付回调处理** | 同一笔订单的状态变更通知（待支付→已支付→已发货）必须严格保序 |
| **IM 消息排序** | 同一会话的消息按 seqNo 顺序投递，同时容忍丢包（通过超时跳过缺口） |
| **游戏服务器消息处理** | 同一玩家的操作指令按序号顺序执行，延迟包通过超时机制被丢弃 |
| **数据库 binlog/CDC 消费** | 同一张表的变更事件按 binlog 位点顺序处理 |
| **微服务异步编排** | 某个业务流水号下的多个异步回调需要按预定顺序执行 |
| **批量任务分片处理** | 同一个分片内的任务必须按序号消费，分片之间可以并发 |

### 什么情况下你不应该使用？

- 需要**全局严格顺序**（所有任务必须串行）—— 这用单线程队列即可
- 需要**持久化 / 消息回溯** —— 应使用 Kafka / RocketMQ 等消息队列
- **session 数量极大**（10万+ 并发会话）—— 每 session 一个消费线程模型会撑爆线程池
- 需要**分布式协调** —— 本库只在单 JVM 进程内工作
- **延迟容忍度极低**（微秒级）—— 时间轮精度在 100ms 级别，且受线程调度影响

---

## 项目规模适用性

### 功能定位

SeqOrderlyConsumer 是一个**库（library）而非中间件**，直接嵌入你的 Java 应用进程内运行。它不依赖外部存储、不依赖消息队列、不依赖任何第三方组件。

### 按项目规模评估

| 项目规模 | 是否适用 | 说明 |
|---|---|---|
| **个人项目 / 原型验证** | ✅ 非常适合 | 零依赖、API 简单、几分钟即可集成 |
| **中小型 Web 应用**（DAU < 10万） | ✅ 适合 | 典型的订单处理、消息通知等场景完全胜任 |
| **中型 SaaS 服务**（DAU 10万-100万） | ⚠️ 需审慎评估 | 需合理配置线程池和超时参数，注意 session 并发量控制 |
| **大型分布式系统**（DAU > 100万） | ❌ 不推荐 | 应考虑使用 RocketMQ / Kafka 等成熟消息中间件做顺序消费 |
| **金融核心交易系统** | ❌ 不推荐 | 缺乏持久化、审计日志、死信队列等金融级特性 |

### 性能参考指标

> 以下数据基于常规硬件环境（4C8G），实际性能取决于任务执行耗时和系统负载。

| 指标 | 参考值 |
|---|---|
| 最大并发 session 数 | 建议 ≤ 1000（受线程池限制） |
| 时间轮精度 | 100ms（可配置） |
| 单 session 吞吐 | 取决于任务逻辑，框架本身开销极小（CAS + 队列操作） |
| 内存占用 | 极低（每 session 仅维护少量元数据） |

---

## 核心能力

```
Session "order-42" 的处理过程：
  submitTask(seqNo=1, execute=true)  ──► 立即执行
  submitTask(seqNo=3, execute=true)  ──► 等待 seqNo=2 ...
       ... 超时后 ...
  submitTask(seqNo=2, execute=false) ──► （迟到，但被跳过）seqNo=3 已被强制推进
```

1. **严格保序**：同一 session 内的任务按照 `seqNo` 严格顺序执行，底层使用 `PriorityBlockingQueue` 排序。
2. **缺口容忍**：允许 seqNo 存在缺口（如收到 1 和 3，缺失 2），超时后自动跳过缺口继续推进，不会永久阻塞。
3. **占位任务**：`execute=false` 的任务仅推进序号而不执行业务逻辑，适合为已知的缺失序号预留位置。
4. **自动清理**：`lastTask=true` 的任务执行完毕后，自动清理该 session 所有资源。
5. **哈希时间轮**：自研的无锁（CAS + 链表）时间轮，O(1) 插入 / 轮询，替代 `ScheduledExecutorService` 在海量定时任务下的性能瓶颈。
6. **单消费者模型**：每个 session 只有一条消费者线程，天然避免并发竞争，消费者通过 `LockSupport.park/unpark` 实现高效唤醒。

---

## 快速开始

```java
// 1. 创建消费者（使用默认配置：1秒/槽、4槽时间轮、缓存线程池）
SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();

// 2. 提交顺序任务到会话 "1001"
consumer.submitTask(() -> step1(), "1001", 1, false, 30, true);
consumer.submitTask(() -> step2(), "1001", 2, false, 30, true);
consumer.submitTask(() -> step3(), "1001", 3, true,  30, true);
// step1 → step2 → step3 顺序执行
// 若 step2 始终未到达，step3 将在 30 秒后强制执行

// 3. 会话完成后自动清理（lastTask=true）或手动清理
consumer.clear("1001");
```

### 使用 `execute=false` 占位

当已知某个序号不会有实际任务时，可以传入一个占位，仅推进序号：

```java
consumer.submitTask(() -> step1(), "s1", 1, false, 30, true);
consumer.submitTask(null,          "s1", 2, false, 30, false); // seqNo=2 被跳过
consumer.submitTask(() -> step3(), "s1", 3, true,  30, true);
// step1 → (seq=2 跳过) → step3
```

### 自定义配置

```java
// 自定义线程池 —— 生产环境推荐
ExecutorService executor = Executors.newFixedThreadPool(20);

// 自定义时间轮 —— 100ms 精度，60 槽（覆盖 6 秒）
ScheduleTimeWheel wheel = new ScheduleTimeWheel(executor);
wheel.setWheel(100, TimeUnit.MILLISECONDS, 60);

SeqOrderlyConsumer consumer = new SeqOrderlyConsumer(wheel, executor);
```

---

## API 参考

### SeqOrderlyConsumer 构造方法

| 构造方法 | 说明 |
|---|---|
| `new SeqOrderlyConsumer()` | 默认：1秒/槽、4槽时间轮、`CachedThreadPool` |
| `new SeqOrderlyConsumer(ExecutorService)` | 自定义线程池，默认时间轮 |
| `new SeqOrderlyConsumer(ScheduleTimeWheel)` | 自定义时间轮，`CachedThreadPool` |
| `new SeqOrderlyConsumer(ScheduleTimeWheel, ExecutorService)` | 完全自定义 |

### submitTask 参数

```java
void submitTask(Runnable runnable, String sessionId, int seqNo,
                boolean lastTask, long waitTimeSecond, boolean execute)
```

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `runnable` | `Runnable` | 是* | 要执行的任务（`execute=false` 时可传 `null`） |
| `sessionId` | `String` | 是 | 会话标识，相同 sessionId 的任务按序执行 |
| `seqNo` | `int` | 是 | 从 1 开始的序号，同一 session 内必须连续递增 |
| `lastTask` | `boolean` | 是 | 是否为该 session 的最后一个任务，`true` 时消费完成后自动清理 |
| `waitTimeSecond` | `long` | 是 | 超时等待时间（秒），当前序号在该时间内未到达时强制执行队头任务 |
| `execute` | `boolean` | 是 | 是否执行业务逻辑；`false` 时仅推进序号，不调用 `runnable.run()` |

### clear

```java
void clear(String sessionId)
```

手动清理指定 session 的所有资源（队列、序号、超时、消费者线程）。通常在不需要 `lastTask` 自动清理的场景下手动调用。

### ScheduleTimeWheel

```java
ScheduleTimeWheel wheel = new ScheduleTimeWheel();
wheel.setWheel(tickInterval, timeUnit, slotCount);              // 配置时间轮
wheel.addDelayTask(callback, delay, timeUnit);                  // 添加延时任务（返回可取消的 DelayTask）
wheel.stop();                                                   // 停止时间轮
```

---

## 架构设计

### 整体结构

```
┌─────────────────────────────────────────────────────────┐
│                  SeqOrderlyConsumer                      │
│                                                          │
│  session-1 ──► PriorityBlockingQueue ──► ConsumerThread │
│  session-2 ──► PriorityBlockingQueue ──► ConsumerThread │
│  session-3 ──► PriorityBlockingQueue ──► ConsumerThread │
│                                                          │
│  超时检测 ◄── ScheduleTimeWheel ◄── DelayTask            │
└─────────────────────────────────────────────────────────┘
```

### 时间轮

```
    slot index:    0     1     2     3
                  │     │     │     │
  指针每 tick ──► [task] [    ] [    ] [task]
                  │     │     │     │
  到期 task ──►  取出执行  跳过  跳过  取出执行
```

自研哈希时间轮解决海量延时任务场景下 `ScheduledExecutorService` 性能不足的问题：
- **O(1) 插入**：通过哈希取模定位槽位，CAS 头插法入队
- **O(1) 轮询**：指针走到哪个槽就处理哪个槽的到期任务
- **无锁 MPSC 队列**：多生产者（`submitTask` 线程）通过 CAS 安全入队，单消费者（轮询线程）安全出队
- **可取消**：`DelayTask.cancel()` 将 deadline 置为 -1，轮询时静默跳过

### 消费者线程模型

每个 session 分配一个独立的消费者线程，优点：
1. 天然避免同一 session 内的并发竞争
2. 任务缺失时通过 `LockSupport.parkNanos` 等待，不浪费 CPU
3. 新任务到达时通过 `LockSupport.unpark` 立即唤醒
4. Session 完成后线程归还线程池

---

## 生产环境评估

### 已达到的生产级特性

| 特性 | 状态 | 说明 |
|---|---|---|
| 线程安全 | ✅ | `ConcurrentHashMap` + `volatile` + CAS + `AtomicBoolean` 全面保障 |
| 无忙等 | ✅ | `LockSupport.parkNanos` / `parkUntil` 替代忙循环 |
| 资源清理 | ✅ | `clear()` 清理所有 session 关联资源 |
| 零外部依赖 | ✅ | 纯 JDK，无任何第三方依赖 |
| 超时兜底 | ✅ | 防止因丢包/乱序导致永久阻塞 |

### 生产环境部署前建议关注

| 项目 | 优先级 | 建议 |
|---|---|---|
| **异常处理** | 🔴 高 | `Runnable.run()` 抛异常会导致消费者线程终止。建议在内部 try-catch 包裹你的业务逻辑 |
| **线程池配置** | 🔴 高 | 默认 `CachedThreadPool` 无上限，生产环境必须使用 `FixedThreadPool` 并合理设置大小（建议 = 预期最大并发 session 数） |
| **日志框架** | 🟡 中 | 当前使用 `System.out.printf`，可考虑替换为 SLF4J |
| **优雅关闭** | 🟡 中 | `SeqOrderlyConsumer` 无 `shutdown()` 方法，建议通过 `ExecutorService` 的 `shutdown` + `awaitTermination` 自行管理 |
| **监控指标** | 🟡 中 | 建议自行添加队列深度、消费速率、超时次数等业务指标 |
| **session 上限** | 🟡 中 | 建议在业务层控制最大并发 session 数，防止线程池耗尽 |
| **时间轮精度** | 🟢 低 | 时间轮 tick 必须是 100ms 的整数倍（`wheelPer % 100 == 0`），对大多数场景够用 |

### 综合评价

> **适用于生产环境**，前提是正确配置线程池并在业务层做好异常兜底。
>
> 本库最适合部署在 **中小型项目的顺序消费场景** 中，它解决的是一个很具体的问题：同一会话内的任务乱序 + 缺口容忍。如果这个正是你的痛点，它比引入一套完整的消息队列要轻便得多。

---

## 最佳实践

### 1. 线程池配置

```java
// 推荐：固定大小线程池，大小 = 预估最大并发 session 数
int maxSessions = 200;
ExecutorService executor = new ThreadPoolExecutor(
    maxSessions, maxSessions,
    60L, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(),
    new ThreadPoolExecutor.CallerRunsPolicy()  // 拒绝策略：由调用线程执行
);
SeqOrderlyConsumer consumer = new SeqOrderlyConsumer(executor);
```

### 2. 业务层异常保护

```java
consumer.submitTask(() -> {
    try {
        // 你的业务逻辑
        doBusinessLogic();
    } catch (Exception e) {
        log.error("任务执行失败", e);
        // 不要在此处抛异常，否则消费者线程会终止
    }
}, "session-1", 1, false, 30, true);
```

### 3. 超时时间设置

- 根据网络延迟和服务响应时间合理设置 `waitTimeSecond`
- 过短：正常延迟的任务被误判为超时，频繁跳过
- 过长：缺失序号导致消费停滞时间过长
- 建议值为 RTT 的 P99 × 3 ~ 5 倍

### 4. Session 生命周期管理

- 使用 `lastTask=true` 标记最后一个任务，框架会自动清理
- 异常场景（如提前中止某笔订单的处理）调用 `clear(sessionId)` 手动清理
- 避免 session 泄漏：确保每个 session 最终都有 `lastTask=true` 或调用 `clear()`

### 5. 不适用场景的替代方案

| 你的需求 | 推荐方案 |
|---|---|
| 全局有序消费 | `LinkedBlockingQueue` + 单线程 |
| 持久化 + 回溯 | Kafka（单分区有序）+ 消费者组 |
| 分布式顺序消费 | RocketMQ 顺序消息 |
| 复杂编排（DAG） | 任务编排框架（如 asyncflow、flowable） |

---

## Maven 依赖

```xml
<dependency>
    <groupId>io.github.magic5689</groupId>
    <artifactId>seq-orderly-consumer</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 环境要求

- Java 21+
- 零运行时依赖

## License

Apache 2.0 — 详见 [LICENSE](LICENSE)
