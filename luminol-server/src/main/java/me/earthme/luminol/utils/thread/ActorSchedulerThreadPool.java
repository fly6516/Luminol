package me.earthme.luminol.utils.thread;

import ca.spottedleaf.concurrentutil.collection.MultiThreadedQueue;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import me.earthme.luminol.utils.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class ActorSchedulerThreadPool {
    public static final long DEADLINE_NOT_SET = Long.MIN_VALUE;

    private final ThreadFactory threadFactory;
    private final MultiThreadedQueue<WorkerThreadCarrier> workers = new MultiThreadedQueue<>();
    private final ConcurrentHashMap<SchedulableTick, WorkerMessageNode> taskBandings = new ConcurrentHashMap<>();
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final Thread managerThread;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // TODO Balance them
    private final long minTaskDeadlineOffset = 1_000_000L; // 1ms
    private final long minTickTimeBuffer = 2_000_000L; // 2ms

    private final long maxTaskDeadlineOffset = 2_000_000L; // 2ms
    private final long maxTickTimeBuffer = 8_000_000L; // 8ms

    private volatile double lastAvgHitPct = 0.5D;

    private boolean booted = false;

    public ActorSchedulerThreadPool(int nThreads, ThreadFactory threadFactory, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.threadFactory = threadFactory;
        this.exceptionHandler = exceptionHandler;

        this.managerThread = this.threadFactory.newThread(this::managerThreadLogic);

        for (int i = 0; i < nThreads; i++) {
            final WorkerThreadCarrier createdWorker = new WorkerThreadCarrier(this.threadFactory);

            this.workers.offer(createdWorker);
        }
    }

    public void start() {
        this.startThreads();
    }

    private void startThreads() {
        this.managerThread.start();
    }

    public void shutdown() {
        this.shutdown.set(true);
    }

    public Thread[] getThreads() {
        final WorkerThreadCarrier[] workers = this.workers.toArray(new WorkerThreadCarrier[0]);

        final Thread[] ret = new Thread[workers.length + 1];

        for (int i = 0; i < workers.length; i++) {
            ret[i] = workers[i].runner;
        }

        ret[ret.length - 1] = this.managerThread;

        return ret;
    }

    public boolean awaitTermination(long time, @NotNull TimeUnit unit) {
        long countDown = unit.toNanos(time);

        while (true) {
            if (!this.managerThread.isAlive()) {
                return true;
            }

            if (countDown <= 0) {
                return false;
            }

            countDown -= 100;

            Thread.yield();
            LockSupport.parkNanos(100);
        }
    }
    
    private void managerThreadLogic() {
        while (!this.shutdown.get()) {
            try {
                if (!this.booted) {
                    for (WorkerThreadCarrier workerThreadCarrier : this.workers) {
                        workerThreadCarrier.kickOff();
                    }

                    this.booted = true;
                }

                final int totalThreads = this.workers.size();

                double totalTaskHitPct = 0;

                for (WorkerThreadCarrier carrier : this.workers) {
                    totalTaskHitPct += (double) carrier.validTaskExecutedCnt.get() / carrier.loopedTimes.get();
                }

                final double avgTaskHitPct = Math.min(totalTaskHitPct / totalThreads, 1.0D);

                this.lastAvgHitPct = avgTaskHitPct;

                final long currToApproach_tickDeadlineOffset = this.minTickTimeBuffer + (long) ((1 - avgTaskHitPct) * (this.maxTickTimeBuffer - this.minTickTimeBuffer));
                final long currToApproach_taskDeadlineSingle = this.minTaskDeadlineOffset + (long) (((this.maxTaskDeadlineOffset - this.minTaskDeadlineOffset)) * (1 - avgTaskHitPct));

                for (WorkerMessageNode node : this.taskBandings.values()) {
                    this.modifyValueOfTask(
                            node,
                            currToApproach_tickDeadlineOffset,
                            currToApproach_taskDeadlineSingle,
                            false, // we don't interrupt as offset its self could do that indirectly
                            false
                    );
                }

                LockSupport.parkNanos(1_000_000L);
            } catch (Exception ex) {
                this.exceptionHandler.uncaughtException(Thread.currentThread(), ex);
            }
        }

        for (WorkerThreadCarrier worker : this.workers) {
            worker.killSignal();
        }

        WorkerThreadCarrier worker;
        while ((worker = this.workers.pollOrBlockAdds()) != null) {
            for (;;) {
                if (worker.status.get() != WorkerThreadCarrier.STATUS_SHUTDOWN) {
                    Thread.yield();
                    LockSupport.parkNanos(1_000_000L);
                    continue;
                }

                break;
            }
        }
    }

    private void modifyValueOfTask(
            WorkerMessageNode target,
            long tickDeadlineOffset,
            long mainThreadTaskPeriod,
            boolean interruptMainThreadTask,
            boolean interruptTick
    ) {
        final Consumer<WorkerMessageNode> action = node -> {
            target.tickTimeDeadlineBuffer = tickDeadlineOffset;
            target.tickTimeDeadlineBuffer = Math.max(this.minTickTimeBuffer, target.tickTimeDeadlineBuffer);
            target.tickTimeDeadlineBuffer = Math.min(this.maxTickTimeBuffer, target.tickTimeDeadlineBuffer);

            target.mainThreadTaskPeriod = mainThreadTaskPeriod;
            target.mainThreadTaskPeriod = Math.max(this.minTaskDeadlineOffset, target.mainThreadTaskPeriod);
            target.mainThreadTaskPeriod = Math.min(this.maxTaskDeadlineOffset, target.mainThreadTaskPeriod);

            // we only do this when we are processed inside a large message block(WorkerMessageNode)
            if (node != null) {
                target.mainThreadTaskInterrupted = interruptMainThreadTask;
                target.tickTaskInterrupted = interruptTick;
            }
        };

        final SubMessageNode wrappedAction = new SubMessageNode(action, target, () -> action.accept(null));

        target.sendMessage(wrappedAction);
        target.notifyReceiver();
    }

    private @NotNull WorkerThreadCarrier selectWorker() {
        WorkerThreadCarrier previousLessTask = null;

        for (WorkerThreadCarrier workerThreadCarrier : this.workers) {
            if (previousLessTask == null) {
                previousLessTask = workerThreadCarrier;
            }

            final int status = workerThreadCarrier.status.get();

            if (status == WorkerThreadCarrier.STATUS_IDLE) {
                return workerThreadCarrier;
            }

            if (previousLessTask.runner == Thread.currentThread()) {
                previousLessTask = null;
                continue;
            }

            if (workerThreadCarrier.validTaskExecutedCnt.get() < previousLessTask.validTaskExecutedCnt.get()) {
                previousLessTask = workerThreadCarrier;
            }
        }

        assert previousLessTask != null;

        return previousLessTask;
    }

    public void notifyTask(SchedulableTick task) {
        final WorkerMessageNode target = this.taskBandings.get(task);

        if (target == null) {
            return;
        }

        // recalculate these
        final long currToApproach_tickDeadlineOffset = this.minTickTimeBuffer + (long) ((1 - this.lastAvgHitPct) * (this.maxTickTimeBuffer - this.minTickTimeBuffer));
        final long currToApproach_taskDeadlineSingle = this.minTaskDeadlineOffset + (long) (((this.maxTaskDeadlineOffset - this.minTaskDeadlineOffset)) * (1 - this.lastAvgHitPct));

        this.modifyValueOfTask(
                target,
                currToApproach_tickDeadlineOffset,
                currToApproach_taskDeadlineSingle,
                false,
                false
        );

        this.dispatchMessageNodeAuto(target);
    }

    public void schedule(SchedulableTick task) {
        final WorkerMessageNode created = new WorkerMessageNode(task);

        this.taskBandings.put(task, created);
        LockSupport.unpark(this.managerThread);

        this.dispatchMessageNodeAuto(created);
    }

    private void removeMessageNode(@NotNull WorkerMessageNode messageNode) {
        this.taskBandings.remove(messageNode.internal);
    }

    private void dispatchMessageNodeAuto(@NotNull WorkerMessageNode workerMessageNode) {
        final WorkerThreadCarrier targetWorker = this.selectWorker();

        // already dispatched by other
        if (!workerMessageNode.tryPreDispatch(targetWorker)) {
            return;
        }

        if (!targetWorker.message(workerMessageNode) && !this.shutdown.get()) {
            workerMessageNode.cleanOwnerWorker(); // we need to reset this to prevent task losing from queue

            this.dispatchMessageNodeAuto(workerMessageNode);
        }
    }

    // copied from concurrentutil
    public static abstract class SchedulableTick {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        public final long id = ID_GENERATOR.getAndIncrement();
        private long scheduledStart = DEADLINE_NOT_SET;


        public final long getScheduledStart() {
            return this.scheduledStart;
        }

        public final void setScheduledStart(final long value) {
            this.scheduledStart = value;
        }

        public abstract boolean runTick();

        public abstract boolean hasTasks();

        public abstract boolean runTasks(final BooleanSupplier canContinue);

        @Override
        public String toString() {
            return "SchedulableTick:{" +
                    "class=" + this.getClass().getName() + ","
                    + "}";
        }
    }

    private final class SubMessageNode {
        private final Consumer<WorkerMessageNode> action;
        @Nullable
        private final WorkerMessageNode insideTask;
        private final Runnable ifFinalized;

        private SubMessageNode(
                Consumer<WorkerMessageNode> action,
                @Nullable WorkerMessageNode insideTask,
                Runnable ifFinalized
        ) {
            this.action = action;
            this.insideTask = insideTask;
            this.ifFinalized = ifFinalized;
        }

        public void process() {
            try {
                this.action.accept(this.insideTask);
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(Thread.currentThread(), ex);
            }
        }

        public void doFinalized() {
            try {
                if (this.ifFinalized != null) {
                    this.ifFinalized.run();
                }
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(Thread.currentThread(), ex);
            }
        }
    }

    private final class WorkerMessageNode {
        private final SchedulableTick internal;
        private final MultiThreadedQueue<SubMessageNode> subMessageNodes = new MultiThreadedQueue<>();

        private long tickTimeDeadlineBuffer = ActorSchedulerThreadPool.this.maxTickTimeBuffer;
        private long mainThreadTaskPeriod = ActorSchedulerThreadPool.this.maxTaskDeadlineOffset;

        private WorkerThreadCarrier ownerWorker;

        private boolean wannaReinsert = true;
        private boolean executed = false;

        private boolean mainThreadTaskInterrupted = false;
        private boolean tickTaskInterrupted = false;

        private static final VarHandle OWNER_HANDLE = ConcurrentUtil.getVarHandle(WorkerMessageNode.class, "ownerWorker", WorkerThreadCarrier.class);

        private WorkerMessageNode(SchedulableTick internal) {
            this.internal = internal;
        }

        public boolean tryPreDispatch(WorkerThreadCarrier carrier) {
            return this.trySetWorker(carrier);
        }

        public boolean trySetWorker(WorkerThreadCarrier ownerWorker) {
            return OWNER_HANDLE.compareAndSet(this, null, ownerWorker);
        }

        private void cleanOwnerWorker() {
            OWNER_HANDLE.setVolatile(this, null);
        }

        private WorkerThreadCarrier getOwnerWorker() {
            return (WorkerThreadCarrier) OWNER_HANDLE.getVolatile(this);
        }

        public void notifyReceiver() {
            final WorkerThreadCarrier owner = (WorkerThreadCarrier) OWNER_HANDLE.getVolatile(this);

            if (owner == null) {
                return;
            }

            LockSupport.unpark(owner.runner);
        }

        private void resetContextFlags() {
            this.mainThreadTaskInterrupted = false;
            this.tickTaskInterrupted = false;
        }

        public void sendMessage(SubMessageNode subMessageNode) {
            this.subMessageNodes.offer(subMessageNode);
        }

        public void doMessageProcess() {
            boolean canceled = false;

            try {
                final long tickDeadline = this.internal.getScheduledStart();
                final long taskDeadline = System.nanoTime() + this.mainThreadTaskPeriod;
                final AtomicInteger executedCount = new AtomicInteger(0);

                if (this.internal.hasTasks()) {
                    canceled = !this.internal.runTasks(() -> {
                        this.processSubMessageNode();

                        final long remaining = System.nanoTime() - taskDeadline;

                        executedCount.incrementAndGet();
                        return remaining > 0 && !this.mainThreadTaskInterrupted;
                    });
                }

                this.executed = executedCount.get() > 0;

                if (canceled) {
                    return;
                }

                long remaining = System.nanoTime() - tickDeadline;

                // we have enough time for this tick, so reinsert back for load balance
                if ((remaining + this.tickTimeDeadlineBuffer) < 0 || this.tickTaskInterrupted) {
                    return;
                }

                for (;;) {
                    this.processSubMessageNode();

                    remaining = System.nanoTime() - tickDeadline;

                    if (remaining < 0) {
                        LockSupport.parkNanos("AWAIT DEADLINE", 1_000L);
                        continue;
                    }

                    canceled = !this.internal.runTick();
                    this.executed = true;
                    break;
                }
            }finally {
                this.finalizeSubMsgBelongToSelf();
                this.resetContextFlags();

                this.wannaReinsert = !canceled;
                this.cleanOwnerWorker();
            }
        }

        private void processSubMessageNode() {
            final SubMessageNode subMessageNode = this.subMessageNodes.poll();

            // might have a interrupt message incoming
            if (subMessageNode != null) {
                subMessageNode.process();
            }
        }

        public void finalizeSubMsgBelongToSelf() {
            SubMessageNode subMessageNode;
            while ((subMessageNode = this.subMessageNodes.poll()) != null) {
                try {
                    subMessageNode.doFinalized();
                }catch (Exception ex) {
                    final WorkerThreadCarrier owner = this.getOwnerWorker();

                    ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(owner != null ? owner.runner : null, ex);
                }
            }
        }
    }

    private final class WorkerThreadCarrier implements Runnable {
        public static final int STATUS_IDLE = 0;
        public static final int STATUS_SHUTDOWN = 1;
        public static final int STATUS_BUSY = 2;
        public static final int STATUS_RUNNING = 3;

        private final Thread runner;
        private final MultiThreadedQueue<WorkerMessageNode> inComingTaskMessages = new MultiThreadedQueue<>();

        private final AtomicBoolean killSignal = new AtomicBoolean(false);
        private final AtomicInteger status = new AtomicInteger(0);

        private final AtomicInteger validTaskExecutedCnt = new AtomicInteger(0);
        private final AtomicInteger loopedTimes = new AtomicInteger(0);

        private WorkerThreadCarrier(@NotNull ThreadFactory factory) {
            runner = factory.newThread(this);
        }

        public void kickOff() {
            this.status.set(STATUS_RUNNING);
            this.runner.start();
        }

        @Override
        public void run() {
            int executeFailureCount = 0;
            for (;;) {
                this.loopedTimes.getAndIncrement();

                final boolean killed = this.killSignal.get();
                final WorkerMessageNode incomingMessage = this.takeMessage(killed);

                // no more task stay in curr thread
                if (killed && incomingMessage == null) {
                    break;
                }

                if (incomingMessage != null) {
                    this.status.set(STATUS_BUSY);

                    Pair<Boolean, Boolean> result = this.processMessage(incomingMessage);

                    final boolean wannaReinsert = result.left();
                    final boolean executed = result.right();

                    if (wannaReinsert) {
                        ActorSchedulerThreadPool.this.dispatchMessageNodeAuto(incomingMessage);
                    }else {
                        ActorSchedulerThreadPool.this.removeMessageNode(incomingMessage);
                    }

                    if (executed) {
                        executeFailureCount = 0;
                        this.validTaskExecutedCnt.incrementAndGet();
                        continue;
                    }
                    executeFailureCount++;
                }

                this.status.set(STATUS_IDLE);

                executeFailureCount++;

                // sleep 1 - 100us
                LockSupport.parkNanos("IDLE", Math.max(Math.max(executeFailureCount, 1), 100) * 1000L);
            }

            this.status.set(STATUS_SHUTDOWN);
        }

        @Contract("_ -> new")
        private @NotNull Pair<Boolean, Boolean> processMessage(@NotNull WorkerMessageNode node) {

            try {
                node.doMessageProcess();
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(this.runner, ex);
            }

            return Pair.of(node.wannaReinsert, node.executed);
        }

        private WorkerMessageNode takeMessage(boolean blockAdd) {
            return blockAdd ? this.inComingTaskMessages.pollOrBlockAdds() : this.inComingTaskMessages.poll();
        }

        private void killSignal() {
            if (this.killSignal.compareAndSet(false, true)) {
                LockSupport.unpark(this.runner);
            }
        }

        private boolean message(WorkerMessageNode messageNode) {
            final boolean queued = this.inComingTaskMessages.offer(messageNode);

            if (queued) {
                LockSupport.unpark(this.runner);
            }

            return queued;
        }
    }
}
