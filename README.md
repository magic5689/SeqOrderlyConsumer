# SeqOrderlyConsumer

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A lightweight, zero-dependency Java library for **session-based sequential message consumption** with timeout fallback, backed by a custom **hashed time wheel**.

## When to use it

- Messages within the same session (e.g., order ID, device ID, user ID) must be processed in strict order
- Gaps may appear — seqNo 1 and 3 arrive, but 2 is delayed or lost
- You want a **timeout**: don't block forever waiting for the missing seqNo

Common scenarios: trading system callbacks, payment result processing, IoT command dispatching, game server message ordering.

## How it works

```
Session "order-42":
  submitTask(seqNo=1) ──► runs immediately
  submitTask(seqNo=3) ──► waits for seqNo=2...
                             │
  ┌─ timeout (30s) ─────────┘
  │  seqNo=3 force-executed
  ▼
```

Internally, a `PriorityBlockingQueue` orders tasks by `seqNo` within each session, while a [hashed time wheel](src/main/java/io/github/magic5689/seqconsumer/ScheduleTimeWheel.java) (lock-free MPSC slots via CAS) triggers timeout fallback so no session gets stuck indefinitely.

## Quick start

```java
// 1. Create a consumer
SeqOrderlyConsumer consumer = new SeqOrderlyConsumer();

// 2. Submit ordered tasks for session "1001"
consumer.submitTask(() -> processStep1(), 1001, 1, false, 30);
consumer.submitTask(() -> processStep2(), 1001, 2, false, 30);
consumer.submitTask(() -> processStep3(), 1001, 3, true,  30);
// Step1 → Step2 → Step3 execute in order.
// If Step2 never arrives, Step3 fires after 30s timeout.

// 3. Cleanup after session completes (optional — auto-cleared on lastTask)
consumer.clear(1001);
```

## API

### SeqOrderlyConsumer

| Constructor | Description |
|---|---|
| `new SeqOrderlyConsumer()` | Default: 1s-tick, 4-slot time wheel, cached thread pool |
| `new SeqOrderlyConsumer(ScheduleTimeWheel)` | Custom time wheel |
| `new SeqOrderlyConsumer(ScheduleTimeWheel, ExecutorService)` | Custom wheel + executor |

```java
void submitTask(Runnable runnable, Integer sessionId, int seqNo,
                boolean lastTask, long waitTimeSecond)
```

- `seqNo` — 1-based, must be consecutive per session
- `lastTask` — signal that this is the final task; consumer auto-clears the session after executing it
- `waitTimeSecond` — how long to wait for a missing seqNo before force-executing remaining tasks

### ScheduleTimeWheel

```java
ScheduleTimeWheel wheel = new ScheduleTimeWheel();
wheel.setWheel(tickInterval, timeUnit, slotCount);
wheel.addDelaTask(callback, delay, timeUnit);  // returns cancellable DelayTask
wheel.stop();
```

## Maven

```xml
<dependency>
    <groupId>io.github.magic5689</groupId>
    <artifactId>seq-orderly-consumer</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Requirements

- Java 21+
- Zero runtime dependencies

## License

Apache 2.0 — see [LICENSE](LICENSE)
