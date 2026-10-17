package com.openagentflow.service;

import com.openagentflow.config.ExecutionContextTaskDecorator;
import com.openagentflow.security.WorkspaceContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import static org.assertj.core.api.Assertions.*;

/** 线程装饰器在正常或异常返回时都不能污染线程原有租户身份。 */
class ExecutionContextTaskDecoratorTests {

    /** 清理当前测试线程。 */
    @AfterEach
    void cleanup() {
        SecurityContextHolder.clearContext();
        WorkspaceContextHolder.clear();
        AsyncTaskExecutionContext.clear();
        MDC.clear();
    }

    /** 任务读取提交方快照，异常结束后恢复执行线程原状态。 */
    @Test
    void propagatesAndRestoresAllContextsOnFailure() {
        bind("submitter", "space-a", "task-a", "request-a");
        Runnable task = new ExecutionContextTaskDecorator().decorate(() -> {
            assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("submitter");
            assertThat(WorkspaceContextHolder.current()).isEqualTo("space-a");
            assertThat(AsyncTaskExecutionContext.current().taskId()).isEqualTo("task-a");
            assertThat(MDC.get("requestId")).isEqualTo("request-a");
            throw new IllegalStateException("task-failed");
        });
        bind("executor", "space-b", "task-b", "request-b");
        assertThatThrownBy(task::run).hasMessage("task-failed");
        assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("executor");
        assertThat(WorkspaceContextHolder.current()).isEqualTo("space-b");
        assertThat(AsyncTaskExecutionContext.current().taskId()).isEqualTo("task-b");
        assertThat(MDC.get("requestId")).isEqualTo("request-b");
    }

    /** 无空间的任务不能继承执行线程前一位用户的空间。 */
    @Test
    void missingContextDoesNotInheritPreviousTenant() {
        Runnable task = new ExecutionContextTaskDecorator().decorate(() -> {
            assertThat(WorkspaceContextHolder.current()).isNull();
            assertThat(AsyncTaskExecutionContext.current()).isNull();
            assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
        });
        bind("executor", "space-b", "task-b", "request-b");
        task.run();
        assertThat(WorkspaceContextHolder.current()).isEqualTo("space-b");
    }

    /** 建立单个线程的完整身份。 */
    private void bind(String user, String workspace, String task, String request) {
        var context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(new UsernamePasswordAuthenticationToken(user, "unused"));
        SecurityContextHolder.setContext(context);
        WorkspaceContextHolder.bind(workspace);
        AsyncTaskExecutionContext.bind(task, "worker", 1L);
        MDC.put("requestId", request);
    }
}
