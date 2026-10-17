package com.openagentflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.domain.chat.*;
import com.openagentflow.domain.knowledge.RagRetrievalOutcome;
import com.openagentflow.domain.model.ModelRouteDecision;
import com.openagentflow.domain.tool.ToolExecutionResult;
import com.openagentflow.entity.*;
import com.openagentflow.mapper.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 聊天混合意图及短事务边界回归测试，不依赖数据库或外部模型。 */
class ChatServiceRegressionTests {

    /** 知识基础设施异常只影响知识子任务，不能阻断独立的合法工具。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mixedIntentStillExecutesToolsWhenRetrievalFails(boolean stream) throws Exception {
        Fixture fixture = new Fixture(true, true, false);
        when(fixture.mock(KnowledgeBaseService.class).retrieveForAgentWithPolicy(any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("vector unavailable"));
        assertThat(fixture.complete(stream)).contains("SHIPPED", "当前知识库资料不足，无法确认").doesNotContain("七天无理由退款");
        assertThat(fixture.executedTools).containsExactly("lookup_order");
        assertThat(fixture.steps.values()).anySatisfy(step -> {
            assertThat(step.getStepType()).isEqualTo("RAG");
            assertThat(step.getStatus()).isEqualTo("FAILED");
        });
    }

    /** 清理同步执行 SSE 任务时恢复的安全上下文。 */
    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
    }

    /** 低可信知识不能阻断已授权工具，也不能透传模型编造的制度结论。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mixedIntentExecutesToolsAndReturnsOnlyActualFacts(boolean stream) throws Exception {
        Fixture fixture = new Fixture(true, true, false);
        String content = fixture.complete(stream);

        assertThat(fixture.executedTools).containsExactly("lookup_order");
        assertThat(content).contains("SHIPPED", "当前知识库资料不足，无法确认")
                .doesNotContain("七天无理由退款");
        assertThat(fixture.modelTransactions).containsOnly(false);
        assertThat(fixture.runs.values()).allSatisfy(run -> assertThat(run.getStatus()).isEqualTo("SUCCESS"));
    }

    /** 未选中或未授权的工具不能因为知识拒答降级而获得执行机会。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ragOnlyStillRefusesWithoutCallingModelOrTools(boolean stream) throws Exception {
        Fixture fixture = new Fixture(false, true, false);

        assertThat(fixture.complete(stream)).contains("当前知识库资料不足，无法确认");
        assertThat(fixture.executedTools).isEmpty();
        assertThat(fixture.modelTransactions).isEmpty();
    }

    /** 混合请求缺少工具实体且知识不足时仍应明确提出澄清问题。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void missingEntityRemainsClarificationEvenWhenKnowledgeIsRejected(boolean stream) throws Exception {
        Fixture fixture = new Fixture(true, true, true);

        assertThat(fixture.complete(stream)).contains("请提供订单号", "当前知识库资料不足，无法确认");
        assertThat(fixture.executedTools).isEmpty();
        assertThat(fixture.modelTransactions).isEmpty();
    }

    /** 可信知识与普通工具链路保留既有模型汇总行为。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void answerableMixedIntentKeepsNormalToolSynthesis(boolean stream) throws Exception {
        Fixture fixture = new Fixture(true, true, false);
        fixture.outcome.setAnswerable(true);

        assertThat(fixture.complete(stream)).contains("七天无理由退款");
        assertThat(fixture.executedTools).containsExactly("lookup_order");
        assertThat(fixture.modelTransactions).hasSize(2).containsOnly(false);
    }

    /** 外层事务回滚不能删除聊天失败日志，外部模型也不能占用该事务。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void modelFailureSurvivesCallerRollbackAndExternalCallHasNoTransaction(boolean stream) throws Exception {
        Fixture fixture = new Fixture(false, false, false);
        doAnswer(invocation -> {
            fixture.recordModelCall();
            throw new IllegalStateException("model unavailable");
        }).when(fixture.mock(OpenAiCompatibleClient.class)).complete(any(), any(), any());
        doAnswer(invocation -> {
            fixture.recordModelCall();
            throw new IllegalStateException("model unavailable");
        }).when(fixture.mock(OpenAiCompatibleClient.class)).completeStream(any(), any(), any(), any());

        fixture.withRolledBackCaller(() -> fixture.complete(stream));

        assertThat(fixture.modelTransactions).containsExactly(false);
        assertThat(fixture.runs.values()).singleElement().satisfies(run -> {
            assertThat(run.getStatus()).isEqualTo("FAILED");
            assertThat(run.getErrorMessage()).contains("model unavailable");
        });
        assertThat(fixture.steps.values()).anySatisfy(step -> assertThat(step.getStatus()).isEqualTo("FAILED"));
        assertThat(fixture.writeTransactions).isNotEmpty().containsOnly(true);
    }

    /** 检索前置异常也应结束运行，不能永远停留在 RUNNING。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retrievalFailurePersistsFailedRunAndRagTrace(boolean stream) throws Exception {
        Fixture fixture = new Fixture(false, true, false);
        when(fixture.mock(KnowledgeBaseService.class).retrieveForAgentWithPolicy(any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("retrieval unavailable"));

        fixture.withRolledBackCaller(() -> fixture.complete(stream));

        assertThat(fixture.runs.values()).singleElement().satisfies(run -> assertThat(run.getStatus()).isEqualTo("FAILED"));
        assertThat(fixture.steps.values()).anySatisfy(step -> {
            assertThat(step.getStepType()).isEqualTo("RAG");
            assertThat(step.getStatus()).isEqualTo("FAILED");
        });
    }

    /** 最终生成失败不能覆盖已成功的工具决策步骤，所有步骤都应有终态。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void finalGenerationFailureMarksFinalStepNotDecision(boolean stream) throws Exception {
        Fixture fixture = new Fixture(true, false, false);
        doThrow(new IllegalStateException("final unavailable")).when(fixture.mock(OpenAiCompatibleClient.class)).complete(any(), any(), any());
        doThrow(new IllegalStateException("final unavailable")).when(fixture.mock(OpenAiCompatibleClient.class)).completeStream(any(), any(), any(), any());

        fixture.withRolledBackCaller(() -> fixture.complete(stream));

        List<RuntimeTraceStepEntity> llmSteps = fixture.steps.values().stream()
                .filter(step -> "LLM".equals(step.getStepType())).toList();
        assertThat(llmSteps).extracting(RuntimeTraceStepEntity::getStatus).containsExactly("SUCCESS", "FAILED");
        assertThat(fixture.runs.values()).singleElement().satisfies(run -> assertThat(run.getStatus()).isEqualTo("FAILED"));
    }

    /** 成功日志同样独立提交，后置记忆处理不能持有日志写事务。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void successSurvivesCallerRollbackAndMemoryRunsOutsideTransaction(boolean stream) throws Exception {
        Fixture fixture = new Fixture(false, false, false);
        fixture.withRolledBackCaller(() -> fixture.complete(stream));

        assertThat(fixture.runs.values()).singleElement().satisfies(run -> assertThat(run.getStatus()).isEqualTo("SUCCESS"));
        assertThat(fixture.modelTransactions).containsExactly(false);
        assertThat(fixture.memoryTransactions).containsExactly(false);
        assertThat(fixture.writeTransactions).containsOnly(true);
    }

    /** 将外部依赖替换为可观察的适配器，但通过真实 Spring 事务代理执行聊天入口。 */
    private static final class Fixture {
        /** 依赖实例按类型注册，兼容新增的事务模板构造参数。 */
        private final Map<Class<?>, Object> dependencies = new HashMap<>();
        /** 仅保存实际提交的运行快照。 */
        private final Map<String, RuntimeRunEntity> runs = new LinkedHashMap<>();
        /** 仅保存实际提交的步骤快照。 */
        private final Map<String, RuntimeTraceStepEntity> steps = new LinkedHashMap<>();
        /** 外部调用与数据库写入的事务状态。 */
        private final List<Boolean> modelTransactions = new ArrayList<>(), memoryTransactions = new ArrayList<>(), writeTransactions = new ArrayList<>();
        /** 实际执行工具名称。 */
        private final List<String> executedTools = new ArrayList<>();
        /** 流式事件正文。 */
        private final StringBuilder deltas = new StringBuilder();
        /** 可按场景修改的检索结果。 */
        private final RagRetrievalOutcome outcome = new RagRetrievalOutcome();
        /** 真实事务模板与被代理的服务。 */
        private final TransactionTemplate transactions = new TransactionTemplate(new LocalTransactionManager());
        /** 被测服务的 Spring 代理。 */
        private final ChatService service;

        /** 准备确定性的路由、工具事实及不可信模型文本。 */
        Fixture(boolean needTool, boolean needRag, boolean clarification) throws Exception {
            ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
            dependencies.put(ObjectMapper.class, mapper);
            dependencies.put(TransactionTemplate.class, transactions);
            dependencies.put(TaskExecutor.class, (TaskExecutor) Runnable::run);
            var constructor = ChatService.class.getConstructors()[0];
            Object[] arguments = Arrays.stream(constructor.getParameterTypes()).map(this::dependency).toArray();
            ChatService target = (ChatService) constructor.newInstance(arguments);
            ProxyFactory proxy = new ProxyFactory(target);
            proxy.addAdvice(new TransactionInterceptor(transactions.getTransactionManager(), new AnnotationTransactionAttributeSource()));
            service = (ChatService) proxy.getProxy();

            AgentEntity agent = new AgentEntity();
            agent.setId("agent-1");
            agent.setWorkspaceId("workspace-1");
            when(mock(AgentMapper.class).selectById("agent-1")).thenReturn(agent);
            ModelConfigEntity model = new ModelConfigEntity();
            model.setId("model-1");
            model.setModelName("test-model");
            model.setMaxOutputTokens(100);
            ModelProviderEntity provider = new ModelProviderEntity();
            provider.setId("provider-1");
            provider.setProviderName("test-provider");
            ModelRouteDecision route = new ModelRouteDecision();
            route.setModel(model);
            route.setProvider(provider);
            when(mock(ModelGatewayService.class).resolveAgentChatRoute(any(), any())).thenReturn(route);
            when(mock(UsageCostService.class).calculateCost(any(), any(), any())).thenReturn(BigDecimal.ZERO);
            when(mock(AiGuardrailService.class).sanitizeOutput(any(), any(), any(), any())).thenAnswer(invocation -> invocation.getArgument(3));
            IntentRoutePlan plan = new IntentRoutePlan();
            plan.setNeedTool(needTool);
            plan.setNeedRag(needRag);
            plan.setNeedsClarification(clarification);
            plan.setClarificationQuestion("请提供订单号");
            plan.setSelectedToolNames(needTool ? List.of("lookup_order") : List.of());
            when(mock(IntentRoutingPolicy.class).plan(any(), any(), anyBoolean())).thenReturn(plan);
            ToolDefinitionForModel tool = new ToolDefinitionForModel();
            tool.setName("lookup_order");
            when(mock(ToolService.class).listModelToolsForAgent(any())).thenReturn(List.of(tool));
            when(mock(KnowledgeBaseService.class).hasEnabledKnowledgeBindings(any())).thenReturn(needRag);
            outcome.setSources(List.of());
            outcome.setTrustedAnswerMode(true);
            outcome.setAnswerable(false);
            outcome.setRejectReason("insufficient citations");
            when(mock(KnowledgeBaseService.class).retrieveForAgentWithPolicy(any(), any(), any(), any())).thenReturn(outcome);

            when(mock(OpenAiCompatibleClient.class).completeWithTools(any(), any(), any())).thenAnswer(invocation -> {
                recordModelCall();
                ToolCallRequest call = new ToolCallRequest();
                call.setId("call-1");
                call.setName("lookup_order");
                call.setArgumentsJson("{\"orderId\":\"123\"}");
                LlmCallResult result = answer();
                result.setToolCalls(List.of(call));
                return result;
            });
            when(mock(OpenAiCompatibleClient.class).complete(any(), any(), any())).thenAnswer(invocation -> {
                recordModelCall();
                return answer();
            });
            when(mock(OpenAiCompatibleClient.class).completeStream(any(), any(), any(), any())).thenAnswer(invocation -> {
                recordModelCall();
                Consumer<String> consumer = invocation.getArgument(3);
                consumer.accept(answer().getContent());
                return answer();
            });
            when(mock(ToolService.class).executeToolCallForAgent(any(), any(), any(), any())).thenAnswer(invocation -> {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                executedTools.add(((ToolCallRequest) invocation.getArgument(3)).getName());
                ToolExecutionResult result = new ToolExecutionResult();
                result.setSuccess(true);
                result.setResponseBody("{\"status\":\"SHIPPED\"}");
                return result;
            });
            doAnswer(invocation -> {
                memoryTransactions.add(TransactionSynchronizationManager.isActualTransactionActive());
                return null;
            }).when(mock(MemoryService.class)).captureConversationMemory(any(), any(), any(), any(), any());
            doAnswer(invocation -> {
                if ("delta".equals(invocation.getArgument(1))) {
                    deltas.append(((Map<?, ?>) invocation.getArgument(2)).get("content"));
                }
                return 1L;
            }).when(mock(RuntimeEventStreamService.class)).publish(any(), any(), any());
            when(mock(RuntimeRunMapper.class).insert(any(RuntimeRunEntity.class))).thenAnswer(invocation -> saveRun(invocation.getArgument(0), mapper));
            when(mock(RuntimeRunMapper.class).updateById(any(RuntimeRunEntity.class))).thenAnswer(invocation -> saveRun(invocation.getArgument(0), mapper));
            when(mock(RuntimeTraceStepMapper.class).insert(any(RuntimeTraceStepEntity.class))).thenAnswer(invocation -> saveStep(invocation.getArgument(0), mapper));
            when(mock(RuntimeTraceStepMapper.class).updateById(any(RuntimeTraceStepEntity.class))).thenAnswer(invocation -> saveStep(invocation.getArgument(0), mapper));
        }

        /** 执行真实的同步或流式入口并返回用户可见文本。 */
        String complete(boolean stream) {
            ChatCompletionRequest request = new ChatCompletionRequest();
            request.setAgentId("agent-1");
            request.setInput("查询订单123并解释退款制度");
            if (stream) {
                service.completeStream(request);
                return deltas.toString();
            }
            return service.complete(request).getContent();
        }

        /** 模拟工作流等调用方回滚，聊天日志仍须独立提交。 */
        void withRolledBackCaller(Runnable action) {
            transactions.executeWithoutResult(status -> {
                try { action.run(); }
                catch (IllegalStateException expected) { /* 业务异常由外层调用方处理。 */ }
                status.setRollbackOnly();
            });
        }

        /** 记录真正进入外部模型适配器时的事务状态。 */
        void recordModelCall() {
            modelTransactions.add(TransactionSynchronizationManager.isActualTransactionActive());
        }

        /** 返回故意包含无来源制度的模型文本，防止降级分支意外泄露。 */
        LlmCallResult answer() {
            LlmCallResult result = new LlmCallResult();
            result.setContent("订单已发货，可享七天无理由退款");
            return result;
        }

        /** 取得构造依赖，只有事务模板和执行器使用真实实现。 */
        Object dependency(Class<?> type) {
            return dependencies.computeIfAbsent(type, key -> org.mockito.Mockito.mock(key));
        }

        /** 取得指定的外部适配器替身。 */
        <T> T mock(Class<T> type) {
            return type.cast(dependency(type));
        }

        /** 在提交后记录不可变运行快照，回滚时不会保留内存实体引用。 */
        int saveRun(RuntimeRunEntity run, ObjectMapper mapper) {
            RuntimeRunEntity snapshot = mapper.convertValue(run, RuntimeRunEntity.class);
            afterCommit(() -> runs.put(snapshot.getId(), snapshot));
            return 1;
        }

        /** 在提交后记录不可变步骤快照。 */
        int saveStep(RuntimeTraceStepEntity step, ObjectMapper mapper) {
            RuntimeTraceStepEntity snapshot = mapper.convertValue(step, RuntimeTraceStepEntity.class);
            afterCommit(() -> steps.put(snapshot.getId(), snapshot));
            return 1;
        }

        /** 利用真实 Spring 提交回调区分独立提交与外层回滚。 */
        void afterCommit(Runnable write) {
            writeTransactions.add(TransactionSynchronizationManager.isActualTransactionActive());
            if (!TransactionSynchronizationManager.isSynchronizationActive()) {
                write.run();
                return;
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() { write.run(); }
            });
        }
    }

    /** 无数据库资源的本地事务管理器，保留 Spring 挂起、恢复、提交和回滚语义。 */
    private static final class LocalTransactionManager extends AbstractPlatformTransactionManager {
        /** 当前线程拥有的事务对象。 */
        private final ThreadLocal<Object> active = new ThreadLocal<>();

        /** 返回当前事务或待开启的新事务对象。 */
        @Override protected Object doGetTransaction() { return active.get() == null ? new Object() : active.get(); }
        /** 判断是否已开启资源事务。 */
        @Override protected boolean isExistingTransaction(Object transaction) { return active.get() == transaction; }
        /** 开启新事务。 */
        @Override protected void doBegin(Object transaction, TransactionDefinition definition) { active.set(transaction); }
        /** 挂起调用方事务。 */
        @Override protected Object doSuspend(Object transaction) { Object value = active.get(); active.remove(); return value; }
        /** 恢复调用方事务。 */
        @Override protected void doResume(Object transaction, Object suspended) { active.set(suspended); }
        /** 提交由 Spring 触发同步回调。 */
        @Override protected void doCommit(DefaultTransactionStatus status) { }
        /** 回滚不执行提交回调。 */
        @Override protected void doRollback(DefaultTransactionStatus status) { }
        /** 清理线程资源。 */
        @Override protected void doCleanupAfterCompletion(Object transaction) { active.remove(); }
    }
}
