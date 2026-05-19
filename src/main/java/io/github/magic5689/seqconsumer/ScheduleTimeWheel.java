package com.example.rpcdemo.SEQ;

import jakarta.annotation.PreDestroy;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

@Getter
public class ScheduleTimeWheel {
    private long wheelPer;
    private MpscTaskQueue[] wheel;

    private final AtomicBoolean flag=new AtomicBoolean(false);//停止标记量
    private final WheelThread wheelThread;
    private final ExecutorService executor;

    public ScheduleTimeWheel(){
        this.wheelThread=new WheelThread();
        this.wheelThread.setDaemon(false);
        this.executor=Executors.newFixedThreadPool(6);
    }

    public ScheduleTimeWheel(ExecutorService executors){
        this.wheelThread=new WheelThread();
        this.wheelThread.setDaemon(false);
        this.executor=executors;
    }



    /**
     * @param wheelPer 时间轮指针动一次所需的时间
     * @param timeUnit 时间轮指针动一次所需的时间单位
     * @param slotCount 总槽数
     */
    public void setWheel(long wheelPer, TimeUnit timeUnit, int slotCount) {
        if (slotCount <= 0) {
            throw new IllegalArgumentException("槽数量必须大于0：" + slotCount);
        }
        this.wheelPer = timeUnit.toMillis(wheelPer);
        if(this.wheelPer%100!=0)throw new IllegalArgumentException("时间轮wheelPer毫秒值必须是100的整数倍");
        wheel = new MpscTaskQueue[slotCount];
        for (int i = 0; i < slotCount; i++) {
            wheel[i]= new MpscTaskQueue();
        }
    }

    /**
     * 停止任务
     */
    @PreDestroy
    public void stop(){
        if (flag.compareAndSet(true, false)) {
            LockSupport.unpark(wheelThread);
        }
        System.out.println("时间轮停止运行");
    }

    /**
     * @param runnable 任务
     * @param delay 延迟时间
     * @param timeUnit 时间单位
     */
    public DelayTask addDelayTask(Runnable runnable,long delay,TimeUnit timeUnit){
        if (runnable == null) {
            throw new NullPointerException("任务runnable不能为null");
        }
        if (delay < 0) {
            throw new IllegalArgumentException("延时时间不能为负数：" + delay);
        }
        if (wheel == null) {
            throw new IllegalStateException("时间轮未初始化");
        }
        //开始执行
        start();

        long delayMs =timeUnit.toMillis(delay);
        DelayTask task= new DelayTask(runnable, delayMs);
        //计算当前任务需要的总共格子数量 向上取整
        int needSlot= (int) (delayMs/this.wheelPer);
        //计算任务所处的槽位
        int taskLocation= (wheelThread.index+needSlot)%this.wheel.length;
        MpscTaskQueue taskQueue = wheel[taskLocation];
        taskQueue.push(task);
        return task;
    }

    private void start() {
        if(flag.compareAndSet(false,true)){
            wheelThread.start();
        }
    }

    private class WheelThread extends Thread {
        //记录时钟走了多少步
        long step=0;
        //基准时间戳
        long baseTime=System.currentTimeMillis();
        Integer index=0;

        @Override
        public void run() {
            while (flag.get()) {
                long currentTime = System.currentTimeMillis();
                long nextTime = baseTime + (step + 1) * wheelPer;
                long waitTime = nextTime - currentTime;
                //槽位索引
                index = Math.toIntExact(step % wheel.length);
                while (waitTime>0) {
                    LockSupport.parkUntil(currentTime + waitTime);
                    currentTime = System.currentTimeMillis();
                    waitTime = nextTime - currentTime;
                    if(!flag.get()){
                        shutdownExecutor();
                        return;
                    }
                }
                //取出任务并执行
                MpscTaskQueue taskQueue = wheel[index];
                List<Runnable> runnables = taskQueue.removeAndReturnShouldRun(nextTime);
                runnables.forEach(executor::execute);
                step++;
                if (step % wheel.length == 0){
                    //时间轮重置
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
                        throw new IllegalArgumentException("时间轮线程池强制关闭失败");
                    }
                }
            }catch (InterruptedException e){
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                throw new IllegalArgumentException("时间轮线程池停止时被中断", e);
            }
            System.out.println("时间轮部线程池停止");;
        }
    }

    public static class DelayTask{
        final Runnable runnable;
        long deadLine;
        DelayTask next;
        public DelayTask(Runnable runnable,long delayMs){
            this.runnable=runnable;
            this.deadLine=System.currentTimeMillis()+delayMs;
        }
        public void cancel(){
            this.deadLine=-1;
        }

    }

    //无锁队列
    private static class MpscTaskQueue {
        //维护的延迟任务链表头节点
        AtomicReference<DelayTask> head=new AtomicReference<>(null);

        public List<Runnable> removeAndReturnShouldRun(long triggerTime) {
            List<Runnable> list=new ArrayList<>();
           DelayTask current = head.get();
           DelayTask pre=null;
            while (current!=null){
                //任务未到指定死亡时间，向下继续遍历
                if(current.deadLine>triggerTime){
                    pre=current;
                    current=current.next;
                    continue;
                }
                //遍历头节点之后的节点，则线程安全 a->b->c
                DelayTask next = current.next;
                if(pre!=null){
                    pre.next=next;
                    if(current.deadLine!=-1) {
                        list.add(current.runnable);
                    }
                    current.next=null;
                    current=next;
                }
                //如果当前节点是头节点，则可能和push冲突，需要CAS设置下一个节点为当前节点
                if(head.compareAndSet(current,next)){
                    if(current!=null) {
                        if(current.deadLine!=-1) {
                            list.add(current.runnable);
                        }
                        current.next = null;
                        current = next;
                        continue;
                    }
                }
                current=head.get();

            }
            return list;
        }

        public void push(DelayTask delayTask) {
            //头插法CAS放入链表
            while (true){
                DelayTask oldHead = head.get();
                delayTask.next=oldHead;
                if(head.compareAndSet(oldHead,delayTask)){
                    return;
                }
            }
        }
    }
}
