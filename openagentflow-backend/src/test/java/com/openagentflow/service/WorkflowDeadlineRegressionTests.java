package com.openagentflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.domain.workflow.WorkflowDtos;
import com.openagentflow.entity.AgentEntity;
import com.openagentflow.entity.RuntimeRunEntity;
import com.openagentflow.entity.WorkflowNodeEntity;
import com.openagentflow.entity.WorkflowStepRunEntity;
import com.openagentflow.mapper.WorkflowStepRunMapper;
import com.openagentflow.security.WorkspaceContextHolder;
import com.openagentflow.workflow.WorkflowNodePlugin;
import com.openagentflow.workflow.WorkflowPluginRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 工作流真实截止时间、取消和深层快照回归测试，不启动 Spring 或后端。 */
class WorkflowDeadlineRegressionTests {

    /** 四个父节点同时等待子节点时，子节点不能排在被父节点占满的池中。 */
    @Test
    void nestedDeadlineDoesNotStarveBehindParents() throws Exception {
        try (com.openagentflow.workflow.WorkflowNodeDeadline deadline = new com.openagentflow.workflow.WorkflowNodeDeadline()) {
            var barrier = new java.util.concurrent.CyclicBarrier(4);
            List<java.util.concurrent.CompletableFuture<String>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                results.add(java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    try {
                        return deadline.call(3000, () -> {
                            barrier.await(2, TimeUnit.SECONDS);
                            return deadline.call(1000, () -> "ok");
                        });
                    } catch (Exception exception) {
                        throw new java.util.concurrent.CompletionException(exception);
                    }
                }));
            }
            for (var result : results) assertThat(result.get(4, TimeUnit.SECONDS)).isEqualTo("ok");
        }
    }

    /** 每个用例结束后清理请求线程上下文。 */
    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
        WorkspaceContextHolder.clear();
        AsyncTaskExecutionContext.clear();
        MDC.clear();
        Thread.interrupted();
    }

    /** 不响应中断的插件仍须及时返回超时，且不得重复其未知副作用。 */
    @Test
    void deadlineReturnsWithoutWaitingForOperationAndDoesNotRepeatSideEffect() throws Exception {
        AtomicInteger effects = new AtomicInteger();
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Fixture fixture = fixture(context -> {
            effects.incrementAndGet();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < until && release.getCount() != 0) {
                try {
                    release.await(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException exception) {
                    interrupted.countDown();
                }
            }
            return "late";
        });
        long started = System.nanoTime();
        Throwable failure;
        try {
            failure = catchThrowable(() -> execute(fixture, node(100, 2), nestedContext()));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(900));
            assertThat(failure).hasMessageContaining("超时");
            assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(effects).hasValue(1);
            ArgumentCaptor<WorkflowStepRunEntity> persisted = ArgumentCaptor.forClass(WorkflowStepRunEntity.class);
            verify(fixture.steps(), atLeastOnce()).updateById(persisted.capture());
            assertThat(persisted.getValue().getStatus()).isEqualTo("FAILED");
            assertThat(persisted.getValue().getRecoverable()).isTrue();
        } finally {
            release.countDown();
        }
    }

    /** 插件取消异常不能被重试策略重复执行。 */
    @Test
    void cancellationDoesNotRetryUnknownPluginSideEffects() throws Exception {
        AtomicInteger effects = new AtomicInteger();
        Fixture fixture = fixture(context -> {
            effects.incrementAndGet();
            throw new CancellationException("cancelled");
        });
        Throwable failure = catchThrowable(() -> execute(fixture, node(500, 3), nestedContext()));
        assertThat(failure).isInstanceOf(CancellationException.class);
        assertThat(effects).hasValue(1);
    }

    /** 截止时间线程应获得请求上下文，同时不能在调用者线程原地执行。 */
    @Test
    void deadlineWorkerReceivesSecurityWorkspaceMdcAndLease() throws Exception {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("user-1", "unused"));
        WorkspaceContextHolder.bind("workspace-1");
        AsyncTaskExecutionContext.bind("task-1", "worker-1", 7);
        MDC.put("requestId", "request-1");
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> worker = new AtomicReference<>();
        Fixture fixture = fixture(context -> {
            worker.set(Thread.currentThread());
            assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("user-1");
            assertThat(WorkspaceContextHolder.current()).isEqualTo("workspace-1");
            assertThat(AsyncTaskExecutionContext.current().lockVersion()).isEqualTo(7);
            assertThat(MDC.get("requestId")).isEqualTo("request-1");
            return "ok";
        });
        execute(fixture, node(1000, 0), nestedContext());
        assertThat(worker.get()).isNotSameAs(caller);
        assertThat(WorkspaceContextHolder.current()).isEqualTo("workspace-1");
        assertThat(AsyncTaskExecutionContext.current().taskId()).isEqualTo("task-1");
    }

    /** 插件收到的嵌套 Map/List 快照不得回写工作流原始上下文。 */
    @Test
    @SuppressWarnings("unchecked")
    void nestedPluginSnapshotCannotMutateParentOrSiblingContext() throws Exception {
        Map<String, Object> parent = nestedContext();
        Fixture fixture = fixture(context -> {
            Map<String, Object> nested = (Map<String, Object>) context.workflowContext().get("nested");
            ((List<String>) nested.get("values")).add("branch-only");
            nested.put("changed", true);
            return "ok";
        });
        execute(fixture, node(0, 0), parent);
        Map<String, Object> nested = (Map<String, Object>) parent.get("nested");
        assertThat((List<String>) nested.get("values")).containsExactly("original");
        assertThat(nested).doesNotContainKey("changed");
    }

    /** 插件 input 与历史节点输出指向同一对象时，也不能通过 input 修改上游数据。 */
    @Test
    @SuppressWarnings("unchecked")
    void pluginInputCannotRewritePreviousNodeOutput() throws Exception {
        Map<String, Object> parent = nestedContext();
        parent.put("previousNode", parent.get("nested"));
        parent.put("lastOutput", parent.get("nested"));
        Fixture fixture = fixture(context -> {
            Map<String, Object> input = (Map<String, Object>) context.input();
            ((List<String>) input.get("values")).add("unexpected");
            return "ok";
        });
        execute(fixture, node(0, 0), parent);
        Map<String, Object> previous = (Map<String, Object>) parent.get("previousNode");
        assertThat((List<String>) previous.get("values")).containsExactly("original");
    }

    /** 调用方中断后必须取消正在运行的操作，并保留中断标志。 */
    @Test
    void callerInterruptionCancelsOperationAndPreservesInterruptFlag() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interruptFlag = new AtomicReference<>(false);
        AtomicInteger effects = new AtomicInteger();
        Fixture fixture = fixture(context -> {
            effects.incrementAndGet();
            entered.countDown();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException exception) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
                throw new CancellationException("interrupted");
            }
            return "late";
        });
        Thread caller = new Thread(() -> {
            failure.set(catchThrowable(() -> execute(fixture, node(5000, 2), nestedContext())));
            interruptFlag.set(Thread.currentThread().isInterrupted());
        });
        caller.start();
        assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
        caller.interrupt();
        caller.join(1000);
        assertThat(caller.isAlive()).isFalse();
        assertThat(failure.get()).isNotNull();
        assertThat(interruptFlag.get()).isTrue();
        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(effects).hasValue(1);
    }

    /** 构造可变嵌套数据，避免仅验证顶层 Map。 */
    private Map<String, Object> nestedContext() {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("lastOutput", "input");
        context.put("nested", new LinkedHashMap<>(Map.of("values", new ArrayList<>(List.of("original")))));
        return context;
    }

    /** 沿用现有节点配置 API。 */
    private WorkflowNodeEntity node(int timeoutMs, int retries) {
        WorkflowNodeEntity node = new WorkflowNodeEntity();
        node.setNodeKey("plugin-1");
        node.setNodeName("deadline-plugin");
        node.setNodeType("PLUGIN");
        node.setConfigJson("{\"pluginCode\":\"test\",\"timeoutMs\":" + timeoutMs + ",\"retryCount\":" + retries + "}");
        return node;
    }

    /** 反射调用现有节点执行边界，避免为测试扩展生产 API。 */
    private void execute(Fixture fixture, WorkflowNodeEntity node, Map<String, Object> context) throws Exception {
        RuntimeRunEntity run = new RuntimeRunEntity();
        run.setId("run-1");
        run.setWorkflowId("workflow-1");
        run.setWorkflowRunId("workflow-run-1");
        Method method = WorkflowExecutionService.class.getDeclaredMethod("executeNode", WorkflowNodeEntity.class,
                AgentEntity.class, RuntimeRunEntity.class, Map.class, WorkflowDtos.RunRequest.class);
        method.setAccessible(true);
        try {
            method.invoke(fixture.service(), node, null, run, context, null);
        } catch (InvocationTargetException exception) {
            if (exception.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw (Error) exception.getCause();
        }
    }

    /** 自动模拟依赖，只保留真实执行引擎、JSON 和插件逻辑。 */
    private Fixture fixture(Function<WorkflowNodePlugin.PluginContext, Object> action) throws Exception {
        Constructor<?> constructor = WorkflowExecutionService.class.getConstructors()[0];
        Object[] args = new Object[constructor.getParameterCount()];
        WorkflowStepRunMapper steps = null;
        for (int index = 0; index < args.length; index++) {
            Class<?> type = constructor.getParameterTypes()[index];
            args[index] = type == ObjectMapper.class ? new ObjectMapper()
                    : type == Executor.class ? (Executor) Runnable::run : mock(type);
            if (type == WorkflowPluginRegistry.class) {
                when(((WorkflowPluginRegistry) args[index]).require("test")).thenReturn(new WorkflowNodePlugin() {
                    @Override
                    public String code() { return "test"; }

                    @Override
                    public Object execute(PluginContext context) { return action.apply(context); }
                });
            }
            if (type == WorkflowStepRunMapper.class) {
                steps = (WorkflowStepRunMapper) args[index];
            }
        }
        return new Fixture((WorkflowExecutionService) constructor.newInstance(args), steps);
    }

    /** 每个用例使用独立引擎与持久化桩。 */
    private record Fixture(WorkflowExecutionService service, WorkflowStepRunMapper steps) { }
}
