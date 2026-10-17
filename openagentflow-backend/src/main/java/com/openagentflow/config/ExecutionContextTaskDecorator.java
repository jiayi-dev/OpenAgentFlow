package com.openagentflow.config;

import com.openagentflow.security.WorkspaceContextHolder;
import com.openagentflow.service.AsyncTaskExecutionContext;
import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;
import org.springframework.security.core.context.SecurityContextHolder;

/** 运行与并行线程统一传递可信身份、空间、租约和日志上下文，并恢复调用前状态。 */
public final class ExecutionContextTaskDecorator implements TaskDecorator {

    /** 捕获提交任务的上下文，避免线程池复用时串用户或丢失租户。 */
    @Override
    public Runnable decorate(Runnable task) {
        var authentication = SecurityContextHolder.getContext().getAuthentication();
        String workspace = WorkspaceContextHolder.current();
        var lease = AsyncTaskExecutionContext.current();
        var logging = MDC.getCopyOfContextMap();
        return () -> {
            var previousSecurity = SecurityContextHolder.getContext();
            String previousWorkspace = WorkspaceContextHolder.current();
            var previousLease = AsyncTaskExecutionContext.current();
            var previousLogging = MDC.getCopyOfContextMap();
            try {
                var security = SecurityContextHolder.createEmptyContext();
                security.setAuthentication(authentication);
                SecurityContextHolder.setContext(security);
                if (workspace == null) WorkspaceContextHolder.clear(); else WorkspaceContextHolder.bind(workspace);
                if (lease == null) AsyncTaskExecutionContext.clear();
                else AsyncTaskExecutionContext.bind(lease.taskId(), lease.workerId(), lease.lockVersion());
                if (logging == null) MDC.clear(); else MDC.setContextMap(logging);
                task.run();
            } finally {
                // CallerRuns 或嵌套执行也恢复原状态，而不是粗暴删除调用者的身份。
                SecurityContextHolder.setContext(previousSecurity);
                if (previousWorkspace == null) WorkspaceContextHolder.clear(); else WorkspaceContextHolder.bind(previousWorkspace);
                if (previousLease == null) AsyncTaskExecutionContext.clear();
                else AsyncTaskExecutionContext.bind(previousLease.taskId(), previousLease.workerId(), previousLease.lockVersion());
                if (previousLogging == null) MDC.clear(); else MDC.setContextMap(previousLogging);
            }
        };
    }
}
