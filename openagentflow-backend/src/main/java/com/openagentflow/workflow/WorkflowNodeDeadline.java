package com.openagentflow.workflow;

import com.openagentflow.exception.BusinessException;
import com.openagentflow.security.WorkspaceContextHolder;
import com.openagentflow.service.AsyncTaskExecutionContext;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** 独立有界节点执行器，截止时间包含排队，取消不会占用并行分支池等待自己。 */
public final class WorkflowNodeDeadline implements AutoCloseable {

    /** 每个工作流服务的节点池，拒绝过载而不是无限排队或回退调用线程。 */
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 16, 30, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(128), task -> {
                Thread thread = new Thread(task, "oaf-workflow-node");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    /** 子流程不能排队等待已被父节点占满的同一线程池；容量耗尽时明确拒绝。 */
    private final ThreadPoolExecutor nestedExecutor = new ThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), task -> {
                Thread thread = new Thread(task, "oaf-workflow-nested-node");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    /** 当前节点取消状态，阻止不响应中断的代码返回后继续提交正常结果。 */
    private static final ThreadLocal<AtomicBoolean> CANCELLED = new ThreadLocal<>();

    /** 在真实截止时间内执行节点，并显式传递请求与任务身份。 */
    public <T> T call(int timeoutMs, Callable<T> operation) throws Exception {
        checkActive();
        if (timeoutMs <= 0) {
            return operation.call();
        }
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String workspace = WorkspaceContextHolder.current();
        Map<String, String> logging = MDC.getCopyOfContextMap();
        AsyncTaskExecutionContext.Lease lease = AsyncTaskExecutionContext.current();
        AtomicBoolean cancelled = new AtomicBoolean();
        ThreadPoolExecutor targetExecutor = CANCELLED.get() == null ? executor : nestedExecutor;
        Future<T> future;
        try {
            future = targetExecutor.submit(() -> {
                try {
                    var security = SecurityContextHolder.createEmptyContext();
                    security.setAuthentication(authentication);
                    SecurityContextHolder.setContext(security);
                    if (workspace != null) WorkspaceContextHolder.bind(workspace);
                    if (logging != null) MDC.setContextMap(logging);
                    if (lease != null) AsyncTaskExecutionContext.bind(lease.taskId(), lease.workerId(), lease.lockVersion());
                    CANCELLED.set(cancelled);
                    checkActive();
                    T value = operation.call();
                    checkActive();
                    return value;
                } finally {
                    CANCELLED.remove();
                    SecurityContextHolder.clearContext();
                    WorkspaceContextHolder.clear();
                    AsyncTaskExecutionContext.clear();
                    MDC.clear();
                }
            });
        } catch (RejectedExecutionException exception) {
            throw new BusinessException("WORKFLOW_NODE_OVERLOADED", "工作流节点执行队列已满，请稍后重试");
        }
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            cancelled.set(true);
            future.cancel(true);
            targetExecutor.purge();
            throw new BusinessException("WORKFLOW_NODE_TIMEOUT", "节点执行超时，已请求取消，外部副作用结果可能未知");
        } catch (InterruptedException exception) {
            cancelled.set(true);
            future.cancel(true);
            targetExecutor.purge();
            Thread.currentThread().interrupt();
            throw exception;
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof Exception cause) throw cause;
            if (exception.getCause() instanceof Error error) throw error;
            throw new IllegalStateException(exception.getCause());
        }
    }

    /** 外部调用返回后、提交节点结果前必须再次检查取消状态。 */
    public static void checkActive() {
        AtomicBoolean flag = CANCELLED.get();
        if (Thread.currentThread().isInterrupted() || (flag != null && flag.get())) {
            throw new CancellationException("工作流节点已取消，禁止提交迟到结果");
        }
    }

    /** 应用退出时取消排队和活动节点。 */
    @Override
    public void close() {
        executor.shutdownNow();
        nestedExecutor.shutdownNow();
    }
}
