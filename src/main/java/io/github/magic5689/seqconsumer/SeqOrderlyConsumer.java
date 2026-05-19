package io.github.magic5689.seqconsumer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class SeqOrderlyConsumer {
    private final ConcurrentHashMap<String,PriorityBlockingQueue<ConsumerTask>> queueMap=new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Integer> seq=new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,TaskConsumerTime> timeOutMap=new ConcurrentHashMap<>();
    private final Map<String, ScheduleTimeWheel.DelayTask> delayTaskMap=new HashMap<>();
    private final ScheduleTimeWheel wheel;
    private ExecutorService EXECUTOR;
    public void clear(String sessionId){
      queueMap.remove(sessionId);
      seq.remove(sessionId);
      timeOutMap.remove(sessionId);
      delayTaskMap.remove(sessionId);
    }

    public SeqOrderlyConsumer(){
        wheel=new ScheduleTimeWheel();
        wheel.setWheel(1, TimeUnit.SECONDS,4);
        EXECUTOR=Executors.newCachedThreadPool();
    }
    public SeqOrderlyConsumer(ScheduleTimeWheel wheel){
        this.wheel=wheel;
    }
    public SeqOrderlyConsumer(ScheduleTimeWheel wheel, ExecutorService executorService){
        this.wheel=wheel;
        EXECUTOR=executorService;
    }

    public SeqOrderlyConsumer(ExecutorService executorService){
        wheel=new ScheduleTimeWheel();
        wheel.setWheel(1, TimeUnit.SECONDS,4);
        EXECUTOR=executorService;
    }

    /**
     * 提交顺序任务。同一 session 内按 seqNo 严格顺序消费，缺失序号时等待超时后强制推进。
     *
     * @param runnable       任务
     * @param sessionId      会话ID（相同会话的任务必须有序）
     * @param seqNo          任务序号，从 1 开始必须连续
     * @param lastTask       是否是最后一个任务（为 true 时消费完成后自动清理该 session）
     * @param waitTimeSecond 超时等待时间（秒）：当前 seqNo 在该时间内未到达时强制执行队头任务
     * @param execute        是否执行任务逻辑；false 时仅推进序号，不调用 runnable
     */
    public void submitTask(Runnable runnable,String sessionId,int seqNo,boolean lastTask,long waitTimeSecond,boolean execute){
        if(seqNo<=0)throw new IllegalArgumentException("sessionId必须大于0");
        seq.putIfAbsent(sessionId, 1);
        PriorityBlockingQueue<ConsumerTask> queue = queueMap.compute(sessionId, (k, v) -> {
            //添加任务到有序队列
            if (v == null) v = new PriorityBlockingQueue<>(10, Comparator.comparingInt(ConsumerTask::getSeqNo));
            ConsumerTask task = new ConsumerTask();
            task.setRunnable(runnable);
            task.setSessionId(sessionId);
            task.setSeqNo(seqNo);
            task.setLastTask(lastTask);
            task.setExecute(execute);
            v.add(task);
            ScheduleTimeWheel.DelayTask delayTask1 = delayTaskMap.get(sessionId);
            //取消当前会话id的超时处理任务
            if(delayTask1!=null)delayTask1.cancel();
            TaskConsumerTime taskConsumerTime=new TaskConsumerTime();
            taskConsumerTime.setWaitTimeSecond(waitTimeSecond);
            taskConsumerTime.setLastConsumerTime(-1);
            timeOutMap.putIfAbsent(sessionId,taskConsumerTime);
            //添加超时处理任务到时间轮
            ScheduleTimeWheel.DelayTask delayTask = wheel.addDelayTask(() -> {
                PriorityBlockingQueue<ConsumerTask> tasks = queueMap.get(sessionId);
                if(tasks==null)return;
                ConsumerTask consumerTask = tasks.poll();
                TaskConsumerTime consumerTime = timeOutMap.get(sessionId);
                if(consumerTime.getLastConsumerTime()==-1)return;
                if(consumerTask==null)return;
                long currentTime = System.currentTimeMillis();
                long lastConsumerTime = consumerTime.getLastConsumerTime();
                if (currentTime > lastConsumerTime) {
                    System.out.printf("任务：%s，序号：%s，等待超时%n", consumerTask.getSessionId(), consumerTask.getSeqNo());
                    seq.computeIfPresent(consumerTask.getSessionId(), (k1, v1) -> v1 + 1);
                    if(consumerTask.isExecute()) {
                        consumerTask.runnable.run();
                    }
                }
            }, waitTimeSecond, TimeUnit.SECONDS);
            //存放当前延时任务的引用
            delayTaskMap.put(sessionId,delayTask);
            return v;
        });
        EXECUTOR.submit(() -> startConsumer(queue));
    }
    private void startConsumer(PriorityBlockingQueue<ConsumerTask> queue){
        while (true) {
            ConsumerTask task = queue.peek();
            if(task==null)break;
            synchronized (task) {
                String sessionId = task.getSessionId();
                Integer currentSeq = seq.get(sessionId);
                if (currentSeq == null) {
                    queue.poll();
                    continue;
                }
                if (seq.get(task.getSessionId()).equals(task.getSeqNo())) {
                    queue.poll();
                    task.runnable.run();
                    if(task.isLastTask()){
                        clear(task.getSessionId());
                        return;
                    }
                    //更新当前任务的消费时间
                    timeOutMap.get(task.getSessionId()).setLastConsumerTime(System.currentTimeMillis());
                    seq.computeIfPresent(task.getSessionId(), (k, v) -> v + 1);
                }
                if (queue.isEmpty()) return;
            }
        }
    }

    private class TaskConsumerTime{
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
    private class ConsumerTask{
        private Runnable runnable;
        private String sessionId;
        private int seqNo;
        private boolean lastTask;
        private boolean execute;

        public boolean isExecute() {
            return execute;
        }

        public void setExecute(boolean execute) {
            this.execute = execute;
        }

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

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }
    }

}
