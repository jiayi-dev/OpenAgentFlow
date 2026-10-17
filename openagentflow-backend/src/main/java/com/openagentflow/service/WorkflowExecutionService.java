package com.openagentflow.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.domain.chat.ChatMessage;
import com.openagentflow.domain.chat.ChatRunContext;
import com.openagentflow.domain.chat.LlmCallResult;
import com.openagentflow.domain.chat.ToolCallRequest;
import com.openagentflow.domain.knowledge.KnowledgeSource;
import com.openagentflow.domain.model.ModelRouteDecision;
import com.openagentflow.domain.prompt.PromptRuntimeDtos;
import com.openagentflow.domain.tool.ToolExecutionResult;
import com.openagentflow.domain.workflow.WorkflowDtos;
import com.openagentflow.entity.AgentEntity;
import com.openagentflow.entity.ModelConfigEntity;
import com.openagentflow.entity.ModelProviderEntity;
import com.openagentflow.entity.RuntimeLlmCallEntity;
import com.openagentflow.entity.RuntimeRunEntity;
import com.openagentflow.entity.RuntimeTraceStepEntity;
import com.openagentflow.entity.WorkflowDefinitionEntity;
import com.openagentflow.entity.WorkflowEdgeEntity;
import com.openagentflow.entity.WorkflowNodeEntity;
import com.openagentflow.entity.WorkflowRunEntity;
import com.openagentflow.entity.WorkflowStepRunEntity;
import com.openagentflow.entity.WorkflowVersionEntity;
import com.openagentflow.exception.BusinessException;
import com.openagentflow.mapper.AgentMapper;
import com.openagentflow.mapper.ModelConfigMapper;
import com.openagentflow.mapper.RuntimeLlmCallMapper;
import com.openagentflow.mapper.RuntimeRunMapper;
import com.openagentflow.mapper.RuntimeTraceStepMapper;
import com.openagentflow.mapper.WorkflowEdgeMapper;
import com.openagentflow.mapper.WorkflowNodeMapper;
import com.openagentflow.mapper.WorkflowRunMapper;
import com.openagentflow.mapper.WorkflowStepRunMapper;
import com.openagentflow.mapper.WorkflowVersionMapper;
import com.openagentflow.workflow.WorkflowGrayReleasePolicy;
import com.openagentflow.workflow.WorkflowNodePlugin;
import com.openagentflow.workflow.WorkflowParallelPolicy;
import com.openagentflow.workflow.WorkflowPluginRegistry;
import com.openagentflow.workflow.WorkflowContextSnapshots;
import com.openagentflow.workflow.WorkflowNodeDeadline;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 工作流执行引擎。
 *
 * <p>支持顺序、条件、循环、并行汇聚、插件、RAG、工具、人工确认和子工作流等生产执行能力。</p>
 */
@Service
public class WorkflowExecutionService {

    /** 模板变量占位符，例如 {{input}}。 */
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{\\s*([a-zA-Z0-9_.-]+)\\s*}}");

    /** 工作流定义服务。 */
    private final WorkflowService workflowService;

    /** 工作流版本 Mapper。 */
    private final WorkflowVersionMapper workflowVersionMapper;

    /** 工作流节点 Mapper。 */
    private final WorkflowNodeMapper workflowNodeMapper;

    /** 工作流连线 Mapper。 */
    private final WorkflowEdgeMapper workflowEdgeMapper;

    /** 工作流运行 Mapper。 */
    private final WorkflowRunMapper workflowRunMapper;

    /** 工作流节点运行 Mapper。 */
    private final WorkflowStepRunMapper workflowStepRunMapper;

    /** Runtime 运行 Mapper。 */
    private final RuntimeRunMapper runtimeRunMapper;

    /** Runtime Trace 步骤 Mapper。 */
    private final RuntimeTraceStepMapper runtimeTraceStepMapper;

    /** Runtime LLM 调用 Mapper。 */
    private final RuntimeLlmCallMapper runtimeLlmCallMapper;

    /** Agent Mapper。 */
    private final AgentMapper agentMapper;

    /** 模型配置 Mapper。 */
    private final ModelConfigMapper modelConfigMapper;

    /** Agent 权限服务。 */
    private final AgentAccessService agentAccessService;

    /** 模型服务商服务。 */
    private final ModelProviderService modelProviderService;

    /** OpenAI-compatible 客户端。 */
    private final OpenAiCompatibleClient openAiCompatibleClient;

    /** RAG 知识库服务。 */
    private final KnowledgeBaseService knowledgeBaseService;

    /** 工具调用服务。 */
    private final ToolService toolService;

    /** 成本与用量服务。 */
    private final UsageCostService usageCostService;

    /** 模型网关服务。 */
    private final ModelGatewayService modelGatewayService;

    /** JDBC 工具，用于写入人工确认任务和策略命中日志。 */
    private final JdbcTemplate jdbcTemplate;

    /** JSON 工具。 */
    private final ObjectMapper objectMapper;

    /** 统一Prompt Runtime服务。 */
    private final PromptRuntimeService promptRuntimeService;

    /** 工作流节点插件注册中心。 */
    private final WorkflowPluginRegistry workflowPluginRegistry;

    /** 有界并行分支执行器。 */
    private final Executor workflowParallelExecutor;

    /** 独立节点截止时间执行器，避免与分支池互相等待。 */
    private final WorkflowNodeDeadline nodeDeadline = new WorkflowNodeDeadline();

    /** 关闭节点池，不遗留后台任务。 */
    @PreDestroy
    public void closeNodeExecutor() { nodeDeadline.close(); }

    public WorkflowExecutionService(WorkflowService workflowService,
                                    WorkflowVersionMapper workflowVersionMapper,
                                    WorkflowNodeMapper workflowNodeMapper,
                                    WorkflowEdgeMapper workflowEdgeMapper,
                                    WorkflowRunMapper workflowRunMapper,
                                    WorkflowStepRunMapper workflowStepRunMapper,
                                    RuntimeRunMapper runtimeRunMapper,
                                    RuntimeTraceStepMapper runtimeTraceStepMapper,
                                    RuntimeLlmCallMapper runtimeLlmCallMapper,
                                    AgentMapper agentMapper,
                                    ModelConfigMapper modelConfigMapper,
                                    AgentAccessService agentAccessService,
                                    ModelProviderService modelProviderService,
                                    OpenAiCompatibleClient openAiCompatibleClient,
                                    KnowledgeBaseService knowledgeBaseService,
                                    ToolService toolService,
                                    UsageCostService usageCostService,
                                    ModelGatewayService modelGatewayService,
                                    JdbcTemplate jdbcTemplate,
                                    ObjectMapper objectMapper,
                                    PromptRuntimeService promptRuntimeService,
                                    WorkflowPluginRegistry workflowPluginRegistry,
                                    @Qualifier("workflowParallelExecutor") Executor workflowParallelExecutor) {
        this.workflowService = workflowService;
        this.workflowVersionMapper = workflowVersionMapper;
        this.workflowNodeMapper = workflowNodeMapper;
        this.workflowEdgeMapper = workflowEdgeMapper;
        this.workflowRunMapper = workflowRunMapper;
        this.workflowStepRunMapper = workflowStepRunMapper;
        this.runtimeRunMapper = runtimeRunMapper;
        this.runtimeTraceStepMapper = runtimeTraceStepMapper;
        this.runtimeLlmCallMapper = runtimeLlmCallMapper;
        this.agentMapper = agentMapper;
        this.modelConfigMapper = modelConfigMapper;
        this.agentAccessService = agentAccessService;
        this.modelProviderService = modelProviderService;
        this.openAiCompatibleClient = openAiCompatibleClient;
        this.knowledgeBaseService = knowledgeBaseService;
        this.toolService = toolService;
        this.usageCostService = usageCostService;
        this.modelGatewayService = modelGatewayService;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.promptRuntimeService = promptRuntimeService;
        this.workflowPluginRegistry = workflowPluginRegistry;
        this.workflowParallelExecutor = workflowParallelExecutor;
    }

    /**
     * 执行指定工作流。
     *
     * @param workflowId 工作流 ID
     * @param request 运行请求
     * @param triggerType 触发类型
     * @return 运行结果
     */
    public WorkflowDtos.RunResult runWorkflow(String workflowId, WorkflowDtos.RunRequest request, String triggerType) {
        WorkflowDefinitionEntity workflow = workflowService.requireWorkflow(workflowId);
        if (!workflowService.canView(workflow)) {
            throw new BusinessException("WORKFLOW_FORBIDDEN", "没有运行该工作流的权限");
        }
        WorkflowDtos.RunResult idempotentResult = findIdempotentRun(workflowId, request);
        if (idempotentResult != null) {
            return idempotentResult;
        }
        AgentEntity agent = resolveAgent(request == null ? null : request.getAgentId());
        WorkflowVersionEntity version = resolveVersion(workflow, request);
        List<WorkflowNodeEntity> nodes = listNodes(workflowId, version);
        List<WorkflowEdgeEntity> edges = listEdges(workflowId, version);
        if (nodes.isEmpty()) {
            throw new BusinessException("WORKFLOW_GRAPH_EMPTY", "工作流没有可执行节点");
        }

        Map<String, Object> context = new LinkedHashMap<>();
        context.put("input", request == null ? "" : safeText(request.getInput()));
        context.put("lastOutput", request == null ? "" : safeText(request.getInput()));
        context.put("variables", request == null || request.getVariables() == null ? Map.of() : request.getVariables());
        context.put("debugMode", request != null && Boolean.TRUE.equals(request.getDebugMode()));
        context.put("dryRun", request != null && Boolean.TRUE.equals(request.getDryRun()));
        context.put("workflowDepth", request == null || request.getVariables() == null ? 0 : numberValue(request.getVariables().get("workflowDepth"), 0D).intValue());
        if (request != null && request.getVariables() != null) {
            // 把常用变量平铺一份，方便节点模板直接用 {{customerId}} 这类写法。
            context.putAll(request.getVariables());
        }

        WorkflowRunEntity workflowRun = createWorkflowRun(workflow, version, agent, request, context, triggerType);
        RuntimeRunEntity runtimeRun = createRuntimeRun(workflow, workflowRun, agent, request, version);
        List<WorkflowDtos.StepResult> stepResults = new ArrayList<>();
        LocalDateTime startedAt = LocalDateTime.now();
        int totalPromptTokens = 0;
        int totalCompletionTokens = 0;
        int totalTokens = 0;
        String finalOutput = safeText(request == null ? "" : request.getInput());

        try {
            WorkflowNodeEntity current = findStartNode(nodes, request == null ? null : request.getStartNodeKey());
            int maxSteps = request == null || request.getMaxSteps() == null ? 100 : Math.max(1, Math.min(request.getMaxSteps(), 500));
            int executedSteps = 0;
            for (int guard = 0; current != null && guard < maxSteps; guard++) {
                executedSteps++;
                NodeExecutionResult nodeResult = executeNode(current, agent, runtimeRun, context, request);
                stepResults.add(nodeResult.stepResult());
                totalPromptTokens += nodeResult.promptTokens();
                totalCompletionTokens += nodeResult.completionTokens();
                totalTokens += nodeResult.totalTokens();
                enforceBudget(workflow, runtimeRun, current, context, totalTokens);
                if (nodeResult.output() != null && !"SKIPPED".equalsIgnoreCase(nodeResult.status())) {
                    context.put("lastOutput", nodeResult.output());
                    finalOutput = String.valueOf(nodeResult.output());
                }
                if ("PARALLEL".equalsIgnoreCase(current.getNodeType())) {
                    ParallelExecutionResult parallel = executeParallelBranches(current, agent, runtimeRun, context,
                            request, nodes, edges, maxSteps);
                    stepResults.addAll(parallel.steps());
                    totalPromptTokens += parallel.promptTokens();
                    totalCompletionTokens += parallel.completionTokens();
                    totalTokens += parallel.totalTokens();
                    context.clear();
                    context.putAll(parallel.context());
                    context.put("lastOutput", parallel.output());
                    finalOutput = String.valueOf(parallel.output());
                    updateWorkflowProgress(workflowRun, current, parallel.joinNode(), context);
                    current = parallel.joinNode();
                    continue;
                }
                WorkflowNodeEntity next = isTerminalNode(current.getNodeType()) ? null : nextNode(current, nodes, edges, context);
                updateWorkflowProgress(workflowRun, current, next, context);
                if ("WAITING".equalsIgnoreCase(nodeResult.status())) {
                    finishWaiting(workflowRun, runtimeRun, context, finalOutput, totalPromptTokens, totalCompletionTokens, totalTokens, startedAt);
                    return toRunResult(workflowRun, runtimeRun, context, finalOutput, stepResults, totalTokens, "等待人工确认");
                }
                if (isTerminalNode(current.getNodeType())) {
                    break;
                }
                current = next;
            }
            if (current != null && executedSteps >= maxSteps && !isTerminalNode(current.getNodeType())) {
                throw new BusinessException("WORKFLOW_MAX_STEPS_EXCEEDED", "工作流超过最大执行步数，已自动中止");
            }
            finishSuccess(workflowRun, runtimeRun, context, finalOutput, totalPromptTokens, totalCompletionTokens, totalTokens, startedAt);
            return toRunResult(workflowRun, runtimeRun, context, finalOutput, stepResults, totalTokens, null);
        } catch (Exception exception) {
            finishFailure(workflowRun, runtimeRun, context, exception, startedAt);
            return toRunResult(workflowRun, runtimeRun, context, finalOutput, stepResults, totalTokens, exception.getMessage());
        }
    }

    /**
     * 查询工作流运行详情。
     *
     * @param workflowRunId 工作流运行ID
     * @return 工作流运行结果快照
     */
    public WorkflowDtos.RunResult getWorkflowRun(String workflowRunId) {
        WorkflowRunEntity workflowRun = requireWorkflowRun(workflowRunId);
        RuntimeRunEntity runtimeRun = findRuntimeRun(workflowRunId);
        return toPersistedRunResult(workflowRun, runtimeRun);
    }

    /**
     * 对失败或等待中的工作流重新发起一次完整运行。
     *
     * @param workflowRunId 来源工作流运行ID
     * @return 新运行结果
     */
    public WorkflowDtos.RunResult retryWorkflowRun(String workflowRunId) {
        WorkflowRunEntity source = requireWorkflowRun(workflowRunId);
        WorkflowDtos.RunRequest retryRequest = buildRunRequestFromSource(source);
        retryRequest.setParentRunId(source.getId());
        retryRequest.setIdempotencyKey(null);
        incrementRetryCount(source);
        return runWorkflow(source.getWorkflowId(), retryRequest, "retry");
    }

    /**
     * 从失败节点或指定节点恢复运行。
     *
     * @param workflowRunId 来源工作流运行ID
     * @param request 恢复请求
     * @return 新运行结果
     */
    public WorkflowDtos.RunResult resumeWorkflowRun(String workflowRunId, WorkflowDtos.RunRequest request) {
        WorkflowRunEntity source = requireWorkflowRun(workflowRunId);
        if (!Boolean.TRUE.equals(source.getRecoverable()) && !StringUtils.hasText(source.getLastNodeKey())) {
            throw new BusinessException("WORKFLOW_RUN_NOT_RECOVERABLE", "该工作流运行不可恢复");
        }
        WorkflowDtos.RunRequest resumeRequest = buildRunRequestFromSource(source);
        if (request != null && StringUtils.hasText(request.getInput())) {
            resumeRequest.setInput(request.getInput());
        }
        if (request != null && request.getVariables() != null) {
            Map<String, Object> variables = new LinkedHashMap<>(resumeRequest.getVariables() == null ? Map.of() : resumeRequest.getVariables());
            variables.putAll(request.getVariables());
            resumeRequest.setVariables(variables);
        }
        String startNodeKey = request != null && StringUtils.hasText(request.getStartNodeKey())
                ? request.getStartNodeKey()
                : firstText(source.getResumeFromNodeKey(), source.getLastNodeKey());
        resumeRequest.setStartNodeKey(startNodeKey);
        resumeRequest.setParentRunId(source.getId());
        resumeRequest.setIdempotencyKey(null);
        resumeRequest.setDebugMode(true);
        incrementRetryCount(source);
        return runWorkflow(source.getWorkflowId(), resumeRequest, "resume");
    }

    /**
     * 执行单个节点。
     *
     * @param node 当前节点
     * @param agent 绑定 Agent
     * @param runtimeRun Runtime 运行记录
     * @param context 工作流上下文
     * @return 节点结果
     */
    private NodeExecutionResult executeNode(WorkflowNodeEntity node,
                                            AgentEntity agent,
                                            RuntimeRunEntity runtimeRun,
                                            Map<String, Object> context,
                                            WorkflowDtos.RunRequest request) throws Exception {
        Map<String, Object> config = parseMap(node.getConfigJson());
        Map<String, Object> retryPolicy = parseMap(node.getRetryPolicy());
        int retryCount = numberValue(firstNonNull(config.get("retryCount"), retryPolicy.get("retryCount")), 0D).intValue();
        int retryIntervalMs = numberValue(firstNonNull(config.get("retryIntervalMs"), retryPolicy.get("retryIntervalMs")), 0D).intValue();
        int timeoutMs = numberValue(firstNonNull(config.get("timeoutMs"), retryPolicy.get("timeoutMs")), 0D).intValue();
        String failureStrategy = stringValue(firstNonNull(config.get("failureStrategy"), retryPolicy.get("failureStrategy")), "STOP").toUpperCase(Locale.ROOT);
        Exception lastException = null;
        NodeConditionDecision conditionDecision = evaluateNodeRunCondition(node, config, context);
        if (!conditionDecision.shouldRun()) {
            WorkflowStepRunEntity stepRun = createWorkflowStepRun(runtimeRun.getWorkflowRunId(), runtimeRun.getWorkflowId(), node, context);
            RuntimeTraceStepEntity traceStep = createTraceStep(runtimeRun, node, context);
            LocalDateTime startedAt = LocalDateTime.now();
            NodeExecutionResult skipped = NodeExecutionResult.skipped(node, conditionDecision.toOutput());
            stepRun.setAttemptNo(1);
            workflowStepRunMapper.updateById(stepRun);
            skipped.stepResult().setAttemptNo(1);
            context.put("lastSkippedNodeKey", node.getNodeKey());
            context.put("lastSkippedReason", conditionDecision.reason());
            context.put("nodeSkipped", true);
            finishStepSuccess(stepRun, traceStep, skipped, startedAt);
            return skipped;
        }
        context.remove("nodeSkipped");

        for (int attempt = 1; attempt <= retryCount + 1; attempt++) {
            WorkflowStepRunEntity stepRun = createWorkflowStepRun(runtimeRun.getWorkflowRunId(), runtimeRun.getWorkflowId(), node, context);
            RuntimeTraceStepEntity traceStep = createTraceStep(runtimeRun, node, context);
            stepRun.setAttemptNo(attempt);
            workflowStepRunMapper.updateById(stepRun);
            LocalDateTime startedAt = LocalDateTime.now();
            try {
                Map<String, Object> attemptContext = WorkflowContextSnapshots.copy(context, objectMapper);
                NodeExecutionResult result = nodeDeadline.call(timeoutMs,
                        () -> executeNodeOnce(node, agent, runtimeRun, traceStep, attemptContext, config, request));
                // 只在本次尝试成功返回后发布上下文，迟到线程只能修改独立副本。
                context.clear();
                context.putAll(attemptContext);
                result.stepResult().setAttemptNo(attempt);
                finishStepSuccess(stepRun, traceStep, result, startedAt);
                return result;
            } catch (Exception exception) {
                lastException = exception;
                finishStepFailure(stepRun, traceStep, exception, startedAt);
                boolean timeout = exception instanceof BusinessException business
                        && "WORKFLOW_NODE_TIMEOUT".equals(business.getCode());
                boolean cancelled = exception instanceof java.util.concurrent.CancellationException
                        || exception instanceof InterruptedException || Thread.currentThread().isInterrupted();
                if (cancelled || (timeout && !Set.of("LLM", "RAG", "START", "END", "CONDITION").contains(node.getNodeType().toUpperCase(Locale.ROOT)))) {
                    writePolicyHit(runtimeRun, node, timeout ? "timeout" : "cancel", "block",
                            Map.of("attempt", attempt, "automaticRetry", false), "结果未知或已取消，禁止自动重试和失败跳转");
                    throw exception;
                }
                if (attempt <= retryCount) {
                    writePolicyHit(runtimeRun, node, "retry", "warn", Map.of("attempt", attempt, "retryCount", retryCount), exception.getMessage());
                    sleepQuietly(retryIntervalMs);
                }
            }
        }

        context.put("error", lastException == null ? "节点执行失败" : lastException.getMessage());
        context.put("lastFailedNode", node.getNodeKey());
        if ("CONTINUE".equals(failureStrategy)) {
            Object fallback = config.getOrDefault("fallbackOutput", "节点失败后按策略继续");
            writePolicyHit(runtimeRun, node, "failure", "fallback", config, "节点失败后继续执行");
            return NodeExecutionResult.failure(node, fallback, lastException == null ? "" : lastException.getMessage(), "FAILED_CONTINUED");
        }
        if ("GOTO".equals(failureStrategy)) {
            String target = stringValue(config.get("failureTargetNodeKey"), "");
            if (StringUtils.hasText(target)) {
                context.put("__nextNodeKey", target);
                writePolicyHit(runtimeRun, node, "failure", "fallback", config, "节点失败后跳转到：" + target);
                return NodeExecutionResult.failure(node, config.getOrDefault("fallbackOutput", ""), lastException == null ? "" : lastException.getMessage(), "FAILED_GOTO");
            }
        }
        throw lastException == null ? new BusinessException("WORKFLOW_NODE_FAILED", "节点执行失败") : lastException;
    }

    /**
     * 执行单次节点逻辑，外层负责重试、超时和失败策略。
     */
    private NodeExecutionResult executeNodeOnce(WorkflowNodeEntity node,
                                                AgentEntity agent,
                                                RuntimeRunEntity runtimeRun,
                                                RuntimeTraceStepEntity traceStep,
                                                Map<String, Object> context,
                                                Map<String, Object> config,
                                                WorkflowDtos.RunRequest request) {
        if (request != null && Boolean.TRUE.equals(request.getDryRun()) && isExternalNode(node.getNodeType())) {
            return dryRunResult(node, config);
        }
        return switch (safeText(node.getNodeType()).toUpperCase(Locale.ROOT)) {
            case "START" -> executeStart(node, context);
            case "RAG" -> executeRag(node, agent, runtimeRun, context, config);
            case "TOOL" -> executeTool(node, agent, runtimeRun, traceStep, context, config);
            case "CONDITION" -> executeCondition(node, context, config);
            case "HUMAN", "APPROVAL" -> executeHuman(node, runtimeRun, context, config);
            case "LOOP", "BATCH" -> executeLoop(node, context, config);
            case "SUBFLOW" -> executeSubflow(node, agent, context, config);
            case "PLUGIN" -> executePlugin(node, agent, runtimeRun, traceStep, context, config);
            case "PARALLEL" -> executeParallel(node, context, config);
            case "JOIN" -> executeJoin(node, context);
            case "API", "WEBHOOK", "NOTIFY" -> executePlugin(node, agent, runtimeRun, traceStep, context, config);
            case "OUTPUT" -> executeOutput(node, context, config);
            case "END" -> executeEnd(node, context);
            default -> executeLlm(node, agent, runtimeRun, traceStep, context, config);
        };
    }

    /**
     * 执行开始节点。
     */
    private NodeExecutionResult executeStart(WorkflowNodeEntity node, Map<String, Object> context) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("input", context.get("input"));
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 执行 RAG 节点。
     */
    private NodeExecutionResult executeRag(WorkflowNodeEntity node,
                                           AgentEntity agent,
                                           RuntimeRunEntity runtimeRun,
                                           Map<String, Object> context,
                                           Map<String, Object> config) {
        if (agent == null) {
            throw new BusinessException("WORKFLOW_AGENT_REQUIRED", "RAG 节点需要绑定 Agent 后运行");
        }
        String queryTemplate = stringValue(config.get("queryTemplate"), "{{input}}");
        String query = renderTemplate(queryTemplate, context);
        List<KnowledgeSource> sources = knowledgeBaseService.retrieveForAgent(agent, query, runtimeRun.getId());
        context.put("sources", sources);
        return NodeExecutionResult.success(node, sources, 0, 0, 0);
    }

    /**
     * 执行 LLM 节点。
     */
    private NodeExecutionResult executeLlm(WorkflowNodeEntity node,
                                           AgentEntity agent,
                                           RuntimeRunEntity runtimeRun,
                                           RuntimeTraceStepEntity traceStep,
                                           Map<String, Object> context,
                                           Map<String, Object> config) {
        ModelRouteDecision routeDecision = modelGatewayService.resolveAgentChatRoute(stringValue(config.get("modelId"), ""), agent);
        ModelConfigEntity model = routeDecision.getModel();
        ModelProviderEntity provider = routeDecision.getProvider();
        ChatRunContext chatContext = new ChatRunContext();
        chatContext.setAgent(agent);
        chatContext.setModel(model);
        chatContext.setProvider(provider);
        chatContext.setApiKey(routeDecision.getApiKey());
        chatContext.setRouteDecision(routeDecision);
        chatContext.setSources(sourcesFromContext(context));
        PromptRuntimeDtos.CompileResult promptCompileResult = compileWorkflowPrompt(agent, config, context, runtimeRun.getId());
        chatContext.setPromptCompileResult(promptCompileResult);
        chatContext.setMessages(buildMessages(agent, config, context, promptCompileResult));
        traceStep.setModelId(model.getId());
        traceStep.setPromptText(toJson(chatContext.getMessages()));

        Double temperature = numberValue(config.get("temperature"), 0.3D);
        Integer maxTokens = numberValue(config.get("maxTokens"), model.getMaxOutputTokens() == null ? 2048D : model.getMaxOutputTokens().doubleValue()).intValue();
        LlmCallResult result = invokeWithGatewayFallback(chatContext,
                current -> openAiCompatibleClient.complete(current, temperature, maxTokens),
                current -> usageCostService.assertWithinQuota(runtimeRun.getUserId(), runtimeRun.getAgentId(), current.getProvider(), current.getModel(), current.getMessages(), maxTokens));
        WorkflowNodeDeadline.checkActive();
        traceStep.setModelId(chatContext.getModel().getId());
        BigDecimal cost = usageCostService.calculateCost(chatContext.getModel(), nullToZero(result.getPromptTokens()), nullToZero(result.getCompletionTokens()));
        traceStep.setCostAmount(cost);
        saveLlmCall(runtimeRun, traceStep, chatContext, result, true, null, cost);
        context.put("answer", safeText(result.getContent()));
        return NodeExecutionResult.success(
                node,
                safeText(result.getContent()),
                nullToZero(result.getPromptTokens()),
                nullToZero(result.getCompletionTokens()),
                nullToZero(result.getTotalTokens()),
                cost
        );
    }

    /**
     * 执行工具节点。
     */
    private NodeExecutionResult executeTool(WorkflowNodeEntity node,
                                            AgentEntity agent,
                                            RuntimeRunEntity runtimeRun,
                                            RuntimeTraceStepEntity traceStep,
                                            Map<String, Object> context,
                                            Map<String, Object> config) {
        if (agent == null) {
            throw new BusinessException("WORKFLOW_AGENT_REQUIRED", "工具节点需要绑定 Agent 后运行");
        }
        String toolCode = resolveToolCode(config);
        if (!StringUtils.hasText(toolCode)) {
            throw new BusinessException("WORKFLOW_TOOL_EMPTY", "工具节点未配置工具编码");
        }
        Map<String, Object> arguments = renderMap(parseMap(config.get("arguments")), context);
        ToolCallRequest call = new ToolCallRequest();
        call.setId("wf_tool_" + newId());
        call.setName(toolCode);
        call.setArgumentsJson(toJson(arguments));
        ToolExecutionResult result = toolService.executeToolCallForAgent(agent, runtimeRun, traceStep.getId(), call);
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("toolName", toolCode);
        output.put("toolCode", toolCode);
        output.put("success", Boolean.TRUE.equals(result.getSuccess()));
        output.put("statusCode", result.getStatusCode());
        output.put("latencyMs", result.getLatencyMs());
        output.put("responseBody", safeText(result.getResponseBody()));
        output.put("responseJson", parseJsonValue(safeText(result.getResponseBody())));
        output.put("errorMessage", safeText(result.getErrorMessage()));
        context.put("toolResult", output);
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 解析工具节点配置中的工具编码。
     *
     * @param config 节点配置
     * @return 工具编码
     */
    private String resolveToolCode(Map<String, Object> config) {
        String directCode = stringValue(config.get("toolCode"), "");
        if (StringUtils.hasText(directCode)) {
            return directCode;
        }
        String toolName = stringValue(config.get("toolName"), "");
        if (StringUtils.hasText(toolName)) {
            return toolName;
        }
        String toolId = stringValue(config.get("toolId"), "");
        if (!StringUtils.hasText(toolId)) {
            return "";
        }
        List<String> codes = jdbcTemplate.queryForList(
                "select tool_code from tool_definition where id = ? and deleted_at is null limit 1",
                String.class,
                toolId
        );
        return codes.isEmpty() ? "" : codes.getFirst();
    }

    /**
     * 执行条件节点。
     */
    private NodeExecutionResult executeCondition(WorkflowNodeEntity node,
                                                 Map<String, Object> context,
                                                 Map<String, Object> config) {
        String expression = stringValue(config.get("conditionExpr"), "success");
        boolean matched = matches(expression, context);
        context.put("conditionMatched", matched);
        return NodeExecutionResult.success(node, Map.of("matched", matched, "expression", expression), 0, 0, 0);
    }

    /**
     * 执行结束节点。
     */
    private NodeExecutionResult executeEnd(WorkflowNodeEntity node, Map<String, Object> context) {
        Object output = context.getOrDefault("lastOutput", context.getOrDefault("answer", ""));
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 执行输出节点，按模板整理最终对话输出。
     */
    private NodeExecutionResult executeOutput(WorkflowNodeEntity node,
                                              Map<String, Object> context,
                                              Map<String, Object> config) {
        String outputTemplate = stringValue(config.get("outputTemplate"), "{{lastOutput}}");
        String output = renderTemplate(outputTemplate, context);
        context.put("answer", output);
        context.put("lastOutput", output);
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 执行人工确认节点，节点会暂停工作流并写入人工任务表。
     */
    private NodeExecutionResult executeHuman(WorkflowNodeEntity node,
                                             RuntimeRunEntity runtimeRun,
                                             Map<String, Object> context,
                                             Map<String, Object> config) {
        String taskId = newId();
        int expireMinutes = numberValue(config.get("expireMinutes"), 60D).intValue();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("nodeKey", node.getNodeKey());
        payload.put("nodeName", node.getNodeName());
        payload.put("context", context);
        payload.put("suggestion", renderTemplate(stringValue(config.get("suggestion"), "{{lastOutput}}"), context));
        jdbcTemplate.update("""
                INSERT INTO workflow_human_task
                  (id, workflow_run_id, step_run_id, task_name, assignee_user_id, payload, status, expired_at)
                VALUES (?, ?, ?, ?, ?, CAST(? AS JSON), 'pending', DATE_ADD(NOW(3), INTERVAL ? MINUTE))
                """,
                taskId,
                runtimeRun.getWorkflowRunId(),
                null,
                stringValue(config.get("taskName"), node.getNodeName()),
                stringValue(config.get("assigneeUserId"), runtimeRun.getUserId()),
                toJson(payload),
                expireMinutes);
        context.put("humanTaskId", taskId);
        return NodeExecutionResult.waiting(node, Map.of("taskId", taskId, "message", "等待人工确认"));
    }

    /**
     * 执行循环/批处理节点，将列表逐项渲染为结果数组。
     */
    private NodeExecutionResult executeLoop(WorkflowNodeEntity node,
                                            Map<String, Object> context,
                                            Map<String, Object> config) {
        Object source = StringUtils.hasText(stringValue(config.get("itemPath"), ""))
                ? pathValue(context, stringValue(config.get("itemPath"), ""))
                : config.get("items");
        Object fallbackSource = source == null ? context.get("lastOutput") : source;
        List<?> items = source instanceof List<?> list
                ? list
                : (fallbackSource == null ? List.of() : List.of(fallbackSource));
        int maxLoops = Math.min(numberValue(config.get("maxLoops"), 20D).intValue(), 200);
        List<Object> outputs = new ArrayList<>();
        String template = stringValue(config.get("itemTemplate"), "{{item}}");
        for (int index = 0; index < Math.min(items.size(), maxLoops); index++) {
            Map<String, Object> itemContext = new LinkedHashMap<>(context);
            itemContext.put("item", items.get(index));
            itemContext.put("index", index);
            outputs.add(renderTemplate(template, itemContext));
        }
        context.put(node.getNodeKey(), outputs);
        context.put("loopResults", outputs);
        return NodeExecutionResult.success(node, outputs, 0, 0, 0);
    }

    /**
     * 执行子工作流节点，可把通用流程封装为可复用节点。
     */
    private NodeExecutionResult executeSubflow(WorkflowNodeEntity node,
                                               AgentEntity agent,
                                               Map<String, Object> context,
                                               Map<String, Object> config) {
        String workflowId = stringValue(config.get("workflowId"), "");
        if (!StringUtils.hasText(workflowId)) {
            throw new BusinessException("WORKFLOW_SUBFLOW_EMPTY", "子工作流节点未配置 workflowId");
        }
        int depth = numberValue(context.get("workflowDepth"), 0D).intValue();
        if (depth >= 3) {
            throw new BusinessException("WORKFLOW_SUBFLOW_DEPTH", "子工作流嵌套层级超过限制");
        }
        WorkflowDtos.RunRequest subRequest = new WorkflowDtos.RunRequest();
        subRequest.setAgentId(agent == null ? null : agent.getId());
        subRequest.setInput(renderTemplate(stringValue(config.get("inputTemplate"), "{{lastOutput}}"), context));
        Map<String, Object> variables = new LinkedHashMap<>(context);
        variables.put("workflowDepth", depth + 1);
        subRequest.setVariables(variables);
        subRequest.setDryRun(Boolean.TRUE.equals(context.get("dryRun")));
        WorkflowDtos.RunResult result = runWorkflow(workflowId, subRequest, "subflow");
        context.put(node.getNodeKey(), result.getOutputText());
        context.put("subflowResult", result);
        return NodeExecutionResult.success(node, result.getOutputText(), 0, 0, result.getTotalTokens() == null ? 0 : result.getTotalTokens());
    }

    /**
     * 执行插件节点；配置工具编码时复用工具执行器，否则调用已注册插件 SPI。
     */
    private NodeExecutionResult executePlugin(WorkflowNodeEntity node,
                                              AgentEntity agent,
                                              RuntimeRunEntity runtimeRun,
                                              RuntimeTraceStepEntity traceStep,
                                              Map<String, Object> context,
                                              Map<String, Object> config) {
        if (StringUtils.hasText(stringValue(config.get("toolName"), "")) || StringUtils.hasText(stringValue(config.get("toolCode"), ""))) {
            return executeTool(node, agent, runtimeRun, traceStep, context, config);
        }
        String pluginCode = stringValue(config.get("pluginCode"), node.getNodeKey());
        WorkflowNodePlugin plugin = workflowPluginRegistry.require(pluginCode);
        Map<String, Object> pluginSnapshot = WorkflowContextSnapshots.copy(context, objectMapper);
        Object output = plugin.execute(new WorkflowNodePlugin.PluginContext(
                pluginSnapshot.get("lastOutput"), java.util.Collections.unmodifiableMap(pluginSnapshot),
                java.util.Collections.unmodifiableMap(WorkflowContextSnapshots.copy(config, objectMapper)), node, agent, runtimeRun));
        context.put(node.getNodeKey(), output);
        context.put("lastPluginCode", pluginCode);
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 执行并行节点入口，实际分支调度由主执行循环完成。
     */
    private NodeExecutionResult executeParallel(WorkflowNodeEntity node,
                                                Map<String, Object> context,
                                                Map<String, Object> config) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("mode", "parallel");
        output.put("strategy", stringValue(config.get("joinStrategy"), "all"));
        output.put("message", "并行分支已进入有界并发执行模式");
        context.put("parallelState", output);
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 并发执行 PARALLEL 节点的全部分支，直到公共 JOIN 节点。
     */
    private ParallelExecutionResult executeParallelBranches(WorkflowNodeEntity parallelNode,
                                                             AgentEntity agent,
                                                             RuntimeRunEntity runtimeRun,
                                                             Map<String, Object> baseContext,
                                                             WorkflowDtos.RunRequest request,
                                                             List<WorkflowNodeEntity> nodes,
                                                             List<WorkflowEdgeEntity> edges,
                                                             int maxSteps) {
        List<WorkflowNodeEntity> branchStarts = edges.stream()
                .filter(edge -> parallelNode.getNodeKey().equals(edge.getSourceNodeKey()))
                .sorted(Comparator.comparing(WorkflowEdgeEntity::getEdgeKey, Comparator.nullsLast(String::compareTo)))
                .map(edge -> findNode(nodes, edge.getTargetNodeKey()))
                .filter(java.util.Objects::nonNull)
                .toList();
        if (branchStarts.size() < 2) {
            throw new BusinessException("WORKFLOW_PARALLEL_BRANCH_REQUIRED", "并行节点至少需要两条有效分支");
        }
        WorkflowNodeEntity joinNode = findCommonJoin(branchStarts, nodes, edges);
        if (joinNode == null) {
            throw new BusinessException("WORKFLOW_PARALLEL_JOIN_REQUIRED", "并行分支必须汇聚到同一个 JOIN 节点");
        }

        List<CompletableFuture<BranchExecutionResult>> futures = branchStarts.stream()
                .map(start -> CompletableFuture.supplyAsync(
                        () -> executeParallelBranch(start, joinNode, agent, runtimeRun,
                                WorkflowContextSnapshots.copy(baseContext, objectMapper), request, nodes, edges, maxSteps),
                        workflowParallelExecutor))
                .toList();

        Map<String, Map<String, Object>> branchContexts = new LinkedHashMap<>();
        List<WorkflowDtos.StepResult> steps = new ArrayList<>();
        Map<String, Object> outputs = new LinkedHashMap<>();
        int promptTokens = 0;
        int completionTokens = 0;
        int totalTokens = 0;
        try {
            // 按分支起点顺序归并，避免线程完成顺序改变上下文和 Trace 展示顺序。
            for (int index = 0; index < branchStarts.size(); index++) {
                BranchExecutionResult branch = futures.get(index).join();
                String branchKey = branchStarts.get(index).getNodeKey();
                branchContexts.put(branchKey, branch.context());
                outputs.put(branchKey, branch.output());
                steps.addAll(branch.steps());
                promptTokens += branch.promptTokens();
                completionTokens += branch.completionTokens();
                totalTokens += branch.totalTokens();
            }
        } catch (CompletionException exception) {
            futures.forEach(future -> future.cancel(true));
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new BusinessException("WORKFLOW_PARALLEL_FAILED", "并行分支执行失败：" + cause.getMessage());
        }
        Map<String, Object> merged = WorkflowParallelPolicy.mergeContexts(baseContext, branchContexts);
        merged.put("parallelOutputs", outputs);
        merged.put("parallelJoinNodeKey", joinNode.getNodeKey());
        return new ParallelExecutionResult(merged, outputs, joinNode, steps,
                promptTokens, completionTokens, totalTokens);
    }

    /** 执行单条并行分支，公共 JOIN 节点由父线程统一执行。 */
    private BranchExecutionResult executeParallelBranch(WorkflowNodeEntity start,
                                                        WorkflowNodeEntity joinNode,
                                                        AgentEntity agent,
                                                        RuntimeRunEntity runtimeRun,
                                                        Map<String, Object> context,
                                                        WorkflowDtos.RunRequest request,
                                                        List<WorkflowNodeEntity> nodes,
                                                        List<WorkflowEdgeEntity> edges,
                                                        int maxSteps) {
        WorkflowNodeEntity current = start;
        List<WorkflowDtos.StepResult> steps = new ArrayList<>();
        int promptTokens = 0;
        int completionTokens = 0;
        int totalTokens = 0;
        Object output = context.get("lastOutput");
        for (int guard = 0; current != null && guard < maxSteps; guard++) {
            if (joinNode.getNodeKey().equals(current.getNodeKey())) {
                break;
            }
            if ("PARALLEL".equalsIgnoreCase(current.getNodeType())) {
                throw new BusinessException("WORKFLOW_NESTED_PARALLEL_UNSUPPORTED", "并行分支内暂不允许再次嵌套并行节点");
            }
            try {
                NodeExecutionResult result = executeNode(current, agent, runtimeRun, context, request);
                if ("WAITING".equalsIgnoreCase(result.status())) {
                    throw new BusinessException("WORKFLOW_PARALLEL_WAITING_UNSUPPORTED", "并行分支不能停留在人工等待节点");
                }
                steps.add(result.stepResult());
                promptTokens += result.promptTokens();
                completionTokens += result.completionTokens();
                totalTokens += result.totalTokens();
                if (result.output() != null && !"SKIPPED".equalsIgnoreCase(result.status())) {
                    output = result.output();
                    context.put("lastOutput", output);
                }
                current = isTerminalNode(current.getNodeType()) ? null : nextNode(current, nodes, edges, context);
            } catch (RuntimeException exception) {
                throw exception;
            } catch (Exception exception) {
                throw new BusinessException("WORKFLOW_PARALLEL_BRANCH_FAILED", "并行分支执行失败：" + exception.getMessage());
            }
        }
        if (current == null || !joinNode.getNodeKey().equals(current.getNodeKey())) {
            throw new BusinessException("WORKFLOW_PARALLEL_JOIN_UNREACHABLE", "并行分支未到达公共 JOIN 节点：" + start.getNodeName());
        }
        return new BranchExecutionResult(context, output, steps, promptTokens, completionTokens, totalTokens);
    }

    /** 查找所有分支都可达的最近 JOIN 节点。 */
    private WorkflowNodeEntity findCommonJoin(List<WorkflowNodeEntity> starts,
                                              List<WorkflowNodeEntity> nodes,
                                              List<WorkflowEdgeEntity> edges) {
        Set<String> common = null;
        for (WorkflowNodeEntity start : starts) {
            Set<String> joins = reachableJoinKeys(start, nodes, edges);
            if (common == null) {
                common = new LinkedHashSet<>(joins);
            } else {
                common.retainAll(joins);
            }
        }
        if (common == null || common.isEmpty()) {
            return null;
        }
        Set<String> commonJoinKeys = Set.copyOf(common);
        return nodes.stream()
                .filter(node -> commonJoinKeys.contains(node.getNodeKey()))
                .filter(node -> "JOIN".equalsIgnoreCase(node.getNodeType()))
                .sorted(Comparator.comparing(WorkflowNodeEntity::getNodeKey))
                .findFirst().orElse(null);
    }

    /** 广度优先收集分支可达的 JOIN 节点，循环图通过 visited 防止重复遍历。 */
    private Set<String> reachableJoinKeys(WorkflowNodeEntity start,
                                          List<WorkflowNodeEntity> nodes,
                                          List<WorkflowEdgeEntity> edges) {
        Set<String> visited = new LinkedHashSet<>();
        Set<String> joins = new LinkedHashSet<>();
        java.util.ArrayDeque<WorkflowNodeEntity> queue = new java.util.ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty() && visited.size() <= 500) {
            WorkflowNodeEntity node = queue.removeFirst();
            if (!visited.add(node.getNodeKey())) {
                continue;
            }
            if ("JOIN".equalsIgnoreCase(node.getNodeType())) {
                joins.add(node.getNodeKey());
                continue;
            }
            edges.stream().filter(edge -> node.getNodeKey().equals(edge.getSourceNodeKey()))
                    .map(edge -> findNode(nodes, edge.getTargetNodeKey()))
                    .filter(java.util.Objects::nonNull)
                    .forEach(queue::addLast);
        }
        return joins;
    }

    private WorkflowNodeEntity findNode(List<WorkflowNodeEntity> nodes, String nodeKey) {
        return nodes.stream().filter(node -> node.getNodeKey().equals(nodeKey)).findFirst().orElse(null);
    }

    /**
     * 执行汇聚节点，汇总上下文中的分支结果。
     */
    private NodeExecutionResult executeJoin(WorkflowNodeEntity node, Map<String, Object> context) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("lastOutput", context.get("lastOutput"));
        output.put("toolResult", context.get("toolResult"));
        output.put("sources", context.get("sources"));
        output.put("loopResults", context.get("loopResults"));
        context.put("joinResult", output);
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 空跑外部节点，避免调试时真实消耗模型、向量库或外部工具。
     */
    private NodeExecutionResult dryRunResult(WorkflowNodeEntity node, Map<String, Object> config) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("dryRun", true);
        output.put("nodeType", node.getNodeType());
        output.put("config", config);
        output.put("message", "调试空跑模式未触发真实外部调用");
        return NodeExecutionResult.success(node, output, 0, 0, 0);
    }

    /**
     * 根据连线选择下一个节点。
     */
    private WorkflowNodeEntity nextNode(WorkflowNodeEntity current,
                                        List<WorkflowNodeEntity> nodes,
                                        List<WorkflowEdgeEntity> edges,
                                        Map<String, Object> context) {
        String forcedTarget = stringValue(context.remove("__nextNodeKey"), "");
        if (StringUtils.hasText(forcedTarget)) {
            return nodes.stream()
                    .filter(node -> forcedTarget.equals(node.getNodeKey()))
                    .findFirst()
                    .orElse(null);
        }
        List<WorkflowEdgeEntity> outgoing = edges.stream()
                .filter(edge -> current.getNodeKey().equals(edge.getSourceNodeKey()))
                .toList();
        if (outgoing.isEmpty()) {
            return null;
        }
        WorkflowEdgeEntity selected = outgoing.get(0);
        if ("CONDITION".equalsIgnoreCase(current.getNodeType())) {
            selected = outgoing.stream()
                    .filter(edge -> matches(edge.getConditionExpr(), context))
                    .findFirst()
                    .orElse(outgoing.stream()
                            .filter(edge -> "default".equalsIgnoreCase(safeText(edge.getConditionExpr()))
                                    || "else".equalsIgnoreCase(safeText(edge.getConditionExpr())))
                            .findFirst()
                            .orElse(outgoing.get(0)));
        }
        String targetKey = selected.getTargetNodeKey();
        return nodes.stream()
                .filter(node -> targetKey.equals(node.getNodeKey()))
                .findFirst()
                .orElse(null);
    }

    /**
     * 评估节点级执行条件，决定当前节点是否进入真实执行。
     */
    private NodeConditionDecision evaluateNodeRunCondition(WorkflowNodeEntity node,
                                                           Map<String, Object> config,
                                                           Map<String, Object> context) {
        boolean enabled = booleanValue(firstNonNull(config.get("runConditionEnabled"), config.get("conditionEnabled")), false);
        String expression = stringValue(firstNonNull(config.get("runConditionExpr"), config.get("nodeConditionExpr")), "").trim();
        String mode = stringValue(firstNonNull(config.get("runConditionMode"), config.get("conditionMode")), "RUN_WHEN").toUpperCase(Locale.ROOT);
        if (!enabled || !StringUtils.hasText(expression)) {
            return new NodeConditionDecision(true, expression, mode, false, "未启用节点执行条件");
        }
        String renderedExpression = renderTemplate(expression, context);
        boolean matched = matches(renderedExpression, context);
        boolean skipWhen = "SKIP_WHEN".equals(mode);
        boolean shouldRun = skipWhen ? !matched : matched;
        String reason = shouldRun
                ? "节点执行条件通过：" + renderedExpression
                : (skipWhen ? "命中跳过条件：" : "未满足执行条件：") + renderedExpression;
        if (!shouldRun) {
            context.put("lastConditionExpression", renderedExpression);
            context.put("lastConditionMatched", matched);
        }
        return new NodeConditionDecision(shouldRun, renderedExpression, mode, matched, reason);
    }

    /**
     * 查询幂等运行结果，避免同一请求被重复执行。
     */
    private WorkflowDtos.RunResult findIdempotentRun(String workflowId, WorkflowDtos.RunRequest request) {
        if (request == null || !StringUtils.hasText(request.getIdempotencyKey())) {
            return null;
        }
        WorkflowRunEntity existing = workflowRunMapper.selectOne(new LambdaQueryWrapper<WorkflowRunEntity>()
                .eq(WorkflowRunEntity::getWorkflowId, workflowId)
                .eq(WorkflowRunEntity::getTriggerUserId, agentAccessService.currentUserId())
                .eq(WorkflowRunEntity::getIdempotencyKey, request.getIdempotencyKey())
                .orderByDesc(WorkflowRunEntity::getCreatedAt)
                .last("limit 1"));
        if (existing == null) {
            return null;
        }
        return toPersistedRunResult(existing, findRuntimeRun(existing.getId()));
    }

    /**
     * 更新工作流运行心跳、最近节点和下一节点。
     */
    private void updateWorkflowProgress(WorkflowRunEntity workflowRun,
                                        WorkflowNodeEntity current,
                                        WorkflowNodeEntity next,
                                        Map<String, Object> context) {
        workflowRun.setLastNodeKey(current == null ? null : current.getNodeKey());
        workflowRun.setNextNodeKey(next == null ? null : next.getNodeKey());
        workflowRun.setHeartbeatAt(LocalDateTime.now());
        workflowRun.setContextJson(toJson(context));
        workflowRun.setSnapshotJson(toJson(Map.of(
                "context", context,
                "lastNodeKey", workflowRun.getLastNodeKey() == null ? "" : workflowRun.getLastNodeKey(),
                "nextNodeKey", workflowRun.getNextNodeKey() == null ? "" : workflowRun.getNextNodeKey()
        )));
        workflowRunMapper.updateById(workflowRun);
        updateLastStepNextNode(workflowRun.getId(), workflowRun.getLastNodeKey(), workflowRun.getNextNodeKey());
    }

    /**
     * 给最近一次步骤运行补充下一节点Key，便于前端展示流向。
     */
    private void updateLastStepNextNode(String workflowRunId, String nodeKey, String nextNodeKey) {
        if (!StringUtils.hasText(workflowRunId) || !StringUtils.hasText(nodeKey)) {
            return;
        }
        WorkflowStepRunEntity latestStep = workflowStepRunMapper.selectOne(new LambdaQueryWrapper<WorkflowStepRunEntity>()
                .eq(WorkflowStepRunEntity::getWorkflowRunId, workflowRunId)
                .eq(WorkflowStepRunEntity::getNodeKey, nodeKey)
                .orderByDesc(WorkflowStepRunEntity::getCreatedAt)
                .last("limit 1"));
        if (latestStep != null) {
            latestStep.setNextNodeKey(nextNodeKey);
            workflowStepRunMapper.updateById(latestStep);
        }
    }

    /**
     * 按ID读取工作流运行，并校验当前用户是否有查看权限。
     */
    private WorkflowRunEntity requireWorkflowRun(String workflowRunId) {
        WorkflowRunEntity workflowRun = workflowRunMapper.selectById(workflowRunId);
        if (workflowRun == null) {
            throw new BusinessException("WORKFLOW_RUN_NOT_FOUND", "工作流运行不存在");
        }
        WorkflowDefinitionEntity workflow = workflowService.requireWorkflow(workflowRun.getWorkflowId());
        if (!workflowService.canView(workflow)) {
            throw new BusinessException("WORKFLOW_FORBIDDEN", "没有查看该工作流运行的权限");
        }
        return workflowRun;
    }

    /**
     * 查找工作流对应的 Runtime Trace 运行。
     */
    private RuntimeRunEntity findRuntimeRun(String workflowRunId) {
        if (!StringUtils.hasText(workflowRunId)) {
            return null;
        }
        return runtimeRunMapper.selectOne(new LambdaQueryWrapper<RuntimeRunEntity>()
                .eq(RuntimeRunEntity::getWorkflowRunId, workflowRunId)
                .orderByDesc(RuntimeRunEntity::getCreatedAt)
                .last("limit 1"));
    }

    /**
     * 将已有运行快照转换成前端可用的运行结果。
     */
    private WorkflowDtos.RunResult toPersistedRunResult(WorkflowRunEntity workflowRun, RuntimeRunEntity runtimeRun) {
        Map<String, Object> context = parseMap(workflowRun.getContextJson());
        Map<String, Object> outputPayload = parseMap(workflowRun.getOutputPayload());
        List<WorkflowDtos.StepResult> steps = workflowStepRunMapper.selectList(new LambdaQueryWrapper<WorkflowStepRunEntity>()
                        .eq(WorkflowStepRunEntity::getWorkflowRunId, workflowRun.getId())
                        .orderByAsc(WorkflowStepRunEntity::getCreatedAt))
                .stream()
                .map(this::toStepResult)
                .toList();
        WorkflowDtos.RunResult result = new WorkflowDtos.RunResult();
        result.setWorkflowRunId(workflowRun.getId());
        result.setRuntimeRunId(runtimeRun == null ? null : runtimeRun.getId());
        result.setWorkflowId(workflowRun.getWorkflowId());
        result.setWorkflowVersionId(workflowRun.getWorkflowVersionId());
        result.setAgentId(workflowRun.getAgentId());
        result.setTriggerType(workflowRun.getTriggerType());
        result.setStatus(workflowRun.getStatus());
        result.setOutputText(stringValue(outputPayload.get("output"), runtimeRun == null ? "" : runtimeRun.getOutputText()));
        result.setContext(context);
        result.setSteps(steps);
        result.setTotalTokens(runtimeRun == null ? 0 : nullToZero(runtimeRun.getTotalTokens()));
        result.setLatencyMs(runtimeRun == null ? null : runtimeRun.getLatencyMs());
        result.setErrorMessage(firstText(workflowRun.getErrorMessage(), runtimeRun == null ? null : runtimeRun.getErrorMessage()));
        result.setIdempotencyKey(workflowRun.getIdempotencyKey());
        result.setParentRunId(workflowRun.getParentRunId());
        result.setResumeFromNodeKey(workflowRun.getResumeFromNodeKey());
        result.setLastNodeKey(workflowRun.getLastNodeKey());
        result.setNextNodeKey(workflowRun.getNextNodeKey());
        result.setRecoverable(workflowRun.getRecoverable());
        result.setRetryCount(nullToZero(workflowRun.getRetryCount()));
        result.setStartedAt(workflowRun.getStartedAt());
        result.setFinishedAt(workflowRun.getFinishedAt());
        return result;
    }

    /**
     * 转换步骤运行快照。
     */
    private WorkflowDtos.StepResult toStepResult(WorkflowStepRunEntity stepRun) {
        WorkflowDtos.StepResult step = new WorkflowDtos.StepResult();
        step.setNodeKey(stepRun.getNodeKey());
        step.setNodeName(stepRun.getNodeName());
        step.setNodeType(stepRun.getNodeType());
        step.setStatus(stepRun.getStatus());
        step.setOutput(parseJsonValue(stepRun.getOutputPayload()));
        step.setTokenCount(nullToZero(stepRun.getTokenCount()));
        step.setLatencyMs(stepRun.getLatencyMs());
        step.setErrorMessage(stepRun.getErrorMessage());
        step.setAttemptNo(nullToZero(stepRun.getAttemptNo()));
        step.setNextNodeKey(stepRun.getNextNodeKey());
        step.setRecoverable(stepRun.getRecoverable());
        step.setStartedAt(stepRun.getStartedAt());
        step.setFinishedAt(stepRun.getFinishedAt());
        return step;
    }

    /**
     * 从历史运行中还原输入参数。
     */
    private WorkflowDtos.RunRequest buildRunRequestFromSource(WorkflowRunEntity source) {
        Map<String, Object> inputPayload = parseMap(source.getInputPayload());
        WorkflowDtos.RunRequest request = new WorkflowDtos.RunRequest();
        request.setAgentId(source.getAgentId());
        request.setInput(stringValue(inputPayload.get("input"), ""));
        request.setVariables(parseMap(inputPayload.get("variables")));
        request.setDebugMode(true);
        request.setMaxSteps(100);
        return request;
    }

    /**
     * 增加来源运行的重跑次数。
     */
    private void incrementRetryCount(WorkflowRunEntity source) {
        source.setRetryCount(nullToZero(source.getRetryCount()) + 1);
        workflowRunMapper.updateById(source);
    }

    /**
     * 创建工作流运行记录。
     */
    private WorkflowRunEntity createWorkflowRun(WorkflowDefinitionEntity workflow,
                                                WorkflowVersionEntity version,
                                                AgentEntity agent,
                                                WorkflowDtos.RunRequest request,
                                                Map<String, Object> context,
                                                String triggerType) {
        WorkflowRunEntity run = new WorkflowRunEntity();
        run.setId(newId());
        run.setWorkflowId(workflow.getId());
        run.setWorkflowVersionId(version == null ? null : version.getId());
        run.setAgentId(agent == null ? null : agent.getId());
        run.setTriggerType(StringUtils.hasText(triggerType) ? triggerType : "manual");
        run.setTriggerUserId(agentAccessService.currentUserId());
        run.setIdempotencyKey(request == null ? null : request.getIdempotencyKey());
        run.setParentRunId(request == null ? null : request.getParentRunId());
        run.setResumeFromNodeKey(request == null ? null : request.getStartNodeKey());
        run.setInputPayload(toJson(Map.of(
                "input", request == null ? "" : safeText(request.getInput()),
                "variables", request == null || request.getVariables() == null ? Map.of() : request.getVariables()
        )));
        run.setContextJson(toJson(context));
        run.setStatus("RUNNING");
        run.setLockedBy(agentAccessService.currentUserId());
        run.setLockedAt(LocalDateTime.now());
        run.setHeartbeatAt(LocalDateTime.now());
        run.setRetryCount(0);
        run.setRecoverable(false);
        run.setSnapshotJson(toJson(Map.of("context", context, "status", "RUNNING")));
        run.setStartedAt(LocalDateTime.now());
        workflowRunMapper.insert(run);
        return run;
    }

    /**
     * 创建 Runtime 运行记录，用于 Trace 页面统一展示。
     */
    private RuntimeRunEntity createRuntimeRun(WorkflowDefinitionEntity workflow,
                                              WorkflowRunEntity workflowRun,
                                              AgentEntity agent,
                                              WorkflowDtos.RunRequest request,
                                              WorkflowVersionEntity version) {
        RuntimeRunEntity run = new RuntimeRunEntity();
        run.setId(newId());
        run.setRunNo("wf_" + DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").format(LocalDateTime.now()));
        run.setRunType("WORKFLOW");
        run.setAgentId(agent == null ? null : agent.getId());
        run.setWorkflowId(workflow.getId());
        run.setWorkflowRunId(workflowRun.getId());
        run.setUserId(agentAccessService.currentUserId());
        run.setInputText(request == null ? "" : safeText(request.getInput()));
        run.setInputPayload(workflowRun.getInputPayload());
        run.setStatus("RUNNING");
        run.setTotalTokens(0);
        run.setPromptTokens(0);
        run.setCompletionTokens(0);
        run.setTotalCost(BigDecimal.ZERO);
        run.setMetadata(toJson(Map.of(
                "workflowName", workflow.getWorkflowName(),
                "versionNo", version == null ? "" : version.getVersionNo()
        )));
        run.setStartedAt(LocalDateTime.now());
        runtimeRunMapper.insert(run);
        return run;
    }

    /**
     * 创建节点运行记录。
     */
    private WorkflowStepRunEntity createWorkflowStepRun(String workflowRunId,
                                                        String workflowId,
                                                        WorkflowNodeEntity node,
                                                        Map<String, Object> context) {
        WorkflowStepRunEntity step = new WorkflowStepRunEntity();
        step.setId(newId());
        step.setWorkflowRunId(workflowRunId);
        step.setWorkflowId(workflowId);
        step.setNodeKey(node.getNodeKey());
        step.setNodeName(node.getNodeName());
        step.setNodeType(node.getNodeType());
        step.setInputPayload(toJson(Map.of("context", context)));
        step.setStatus("RUNNING");
        step.setAttemptNo(1);
        step.setTokenCount(0);
        step.setCostAmount(BigDecimal.ZERO);
        step.setRecoverable(false);
        step.setPolicySnapshot(toJson(Map.of(
                "config", parseMap(node.getConfigJson()),
                "retryPolicy", parseMap(node.getRetryPolicy())
        )));
        step.setStartedAt(LocalDateTime.now());
        workflowStepRunMapper.insert(step);
        return step;
    }

    /**
     * 创建 Runtime Trace 步骤。
     */
    private RuntimeTraceStepEntity createTraceStep(RuntimeRunEntity run, WorkflowNodeEntity node, Map<String, Object> context) {
        RuntimeTraceStepEntity step = new RuntimeTraceStepEntity();
        step.setId(newId());
        step.setRunId(run.getId());
        step.setStepKey(node.getNodeKey());
        step.setStepName(node.getNodeName());
        step.setStepType(safeText(node.getNodeType()).toUpperCase(Locale.ROOT));
        step.setStatus("RUNNING");
        step.setInputPayload(toJson(Map.of("context", context, "nodeConfig", parseMap(node.getConfigJson()))));
        step.setTokenUsage("{}");
        step.setCostAmount(BigDecimal.ZERO);
        step.setStartedAt(LocalDateTime.now());
        runtimeTraceStepMapper.insert(step);
        return step;
    }

    /**
     * 完成节点成功状态。
     */
    private void finishStepSuccess(WorkflowStepRunEntity stepRun,
                                   RuntimeTraceStepEntity traceStep,
                                   NodeExecutionResult result,
                                   LocalDateTime startedAt) {
        int latencyMs = (int) Duration.between(startedAt, LocalDateTime.now()).toMillis();
        stepRun.setStatus(result.status());
        stepRun.setOutputPayload(toJson(result.output()));
        stepRun.setTokenCount(result.totalTokens());
        stepRun.setCostAmount(result.costAmount());
        stepRun.setErrorMessage(result.errorMessage());
        stepRun.setLatencyMs(latencyMs);
        stepRun.setRecoverable(false);
        stepRun.setFinishedAt(LocalDateTime.now());
        workflowStepRunMapper.updateById(stepRun);

        traceStep.setStatus(result.status());
        traceStep.setOutputPayload(toJson(result.output()));
        traceStep.setErrorMessage(result.errorMessage());
        traceStep.setTokenUsage(toJson(Map.of(
                "promptTokens", result.promptTokens(),
                "completionTokens", result.completionTokens(),
                "totalTokens", result.totalTokens()
        )));
        traceStep.setCostAmount(result.costAmount());
        traceStep.setLatencyMs(latencyMs);
        traceStep.setFinishedAt(LocalDateTime.now());
        runtimeTraceStepMapper.updateById(traceStep);
        result.stepResult().setLatencyMs(latencyMs);
    }

    /**
     * 完成节点失败状态。
     */
    private void finishStepFailure(WorkflowStepRunEntity stepRun,
                                   RuntimeTraceStepEntity traceStep,
                                   Exception exception,
                                   LocalDateTime startedAt) {
        int latencyMs = (int) Duration.between(startedAt, LocalDateTime.now()).toMillis();
        stepRun.setStatus("FAILED");
        stepRun.setErrorMessage(exception.getMessage());
        stepRun.setLatencyMs(latencyMs);
        stepRun.setRecoverable(true);
        stepRun.setFinishedAt(LocalDateTime.now());
        workflowStepRunMapper.updateById(stepRun);

        traceStep.setStatus("FAILED");
        traceStep.setErrorMessage(exception.getMessage());
        traceStep.setLatencyMs(latencyMs);
        traceStep.setFinishedAt(LocalDateTime.now());
        runtimeTraceStepMapper.updateById(traceStep);
    }

    /**
     * 完成工作流成功状态。
     */
    private void finishSuccess(WorkflowRunEntity workflowRun,
                               RuntimeRunEntity runtimeRun,
                               Map<String, Object> context,
                               String output,
                               int promptTokens,
                               int completionTokens,
                               int totalTokens,
                               LocalDateTime startedAt) {
        int latencyMs = (int) Duration.between(startedAt, LocalDateTime.now()).toMillis();
        workflowRun.setStatus("SUCCESS");
        workflowRun.setContextJson(toJson(context));
        workflowRun.setOutputPayload(toJson(Map.of("output", safeText(output))));
        workflowRun.setNextNodeKey(null);
        workflowRun.setHeartbeatAt(LocalDateTime.now());
        workflowRun.setRecoverable(false);
        workflowRun.setSnapshotJson(toJson(Map.of("context", context, "output", safeText(output), "status", "SUCCESS")));
        workflowRun.setFinishedAt(LocalDateTime.now());
        workflowRunMapper.updateById(workflowRun);

        runtimeRun.setStatus("SUCCESS");
        runtimeRun.setOutputText(safeText(output));
        runtimeRun.setOutputPayload(toJson(Map.of("output", safeText(output), "context", context)));
        runtimeRun.setPromptTokens(promptTokens);
        runtimeRun.setCompletionTokens(completionTokens);
        runtimeRun.setTotalTokens(totalTokens);
        runtimeRun.setTotalCost(sumWorkflowCost(runtimeRun.getId()));
        runtimeRun.setLatencyMs(latencyMs);
        runtimeRun.setFinishedAt(LocalDateTime.now());
        runtimeRunMapper.updateById(runtimeRun);
    }

    /**
     * 完成工作流等待人工确认状态。
     */
    private void finishWaiting(WorkflowRunEntity workflowRun,
                               RuntimeRunEntity runtimeRun,
                               Map<String, Object> context,
                               String output,
                               int promptTokens,
                               int completionTokens,
                               int totalTokens,
                               LocalDateTime startedAt) {
        int latencyMs = (int) Duration.between(startedAt, LocalDateTime.now()).toMillis();
        workflowRun.setStatus("WAITING");
        workflowRun.setContextJson(toJson(context));
        workflowRun.setOutputPayload(toJson(Map.of("output", safeText(output), "waiting", true)));
        workflowRun.setHeartbeatAt(LocalDateTime.now());
        workflowRun.setRecoverable(true);
        workflowRun.setSnapshotJson(toJson(Map.of("context", context, "output", safeText(output), "status", "WAITING")));
        workflowRun.setFinishedAt(null);
        workflowRunMapper.updateById(workflowRun);

        runtimeRun.setStatus("WAITING");
        runtimeRun.setOutputText(safeText(output));
        runtimeRun.setOutputPayload(toJson(Map.of("output", safeText(output), "context", context, "waiting", true)));
        runtimeRun.setPromptTokens(promptTokens);
        runtimeRun.setCompletionTokens(completionTokens);
        runtimeRun.setTotalTokens(totalTokens);
        runtimeRun.setTotalCost(sumWorkflowCost(runtimeRun.getId()));
        runtimeRun.setLatencyMs(latencyMs);
        runtimeRunMapper.updateById(runtimeRun);
    }

    /**
     * 完成工作流失败状态。
     */
    private void finishFailure(WorkflowRunEntity workflowRun,
                               RuntimeRunEntity runtimeRun,
                               Map<String, Object> context,
                               Exception exception,
                               LocalDateTime startedAt) {
        int latencyMs = (int) Duration.between(startedAt, LocalDateTime.now()).toMillis();
        workflowRun.setStatus("FAILED");
        workflowRun.setContextJson(toJson(context));
        workflowRun.setErrorMessage(exception.getMessage());
        workflowRun.setHeartbeatAt(LocalDateTime.now());
        workflowRun.setRecoverable(true);
        workflowRun.setSnapshotJson(toJson(Map.of("context", context, "error", exception.getMessage(), "status", "FAILED")));
        workflowRun.setFinishedAt(LocalDateTime.now());
        workflowRunMapper.updateById(workflowRun);

        runtimeRun.setStatus("FAILED");
        runtimeRun.setErrorMessage(exception.getMessage());
        runtimeRun.setLatencyMs(latencyMs);
        runtimeRun.setFinishedAt(LocalDateTime.now());
        runtimeRunMapper.updateById(runtimeRun);
    }

    /**
     * 保存 LLM 调用日志。
     */
    /**
     * 通过模型网关执行工作流 LLM 调用，并在允许时自动切换候选模型。
     *
     * @param context 聊天运行上下文
     * @param invoker 实际模型调用函数
     * @param precheck 调用前预检查
     * @return LLM 调用结果
     */
    private LlmCallResult invokeWithGatewayFallback(ChatRunContext context,
                                                    Function<ChatRunContext, LlmCallResult> invoker,
                                                    Consumer<ChatRunContext> precheck) {
        try {
            precheck.accept(context);
            return invoker.apply(context);
        } catch (Exception firstException) {
            // 取消或截止时间已到时不能通过网关回退再次发起模型请求。
            WorkflowNodeDeadline.checkActive();
            ModelRouteDecision fallback = modelGatewayService.nextFallbackDecision(context.getRouteDecision(), firstException.getMessage());
            if (fallback == null) {
                throw firstException;
            }
            // 回退后用新的模型上下文继续执行，后续日志会记录实际模型。
            context.setRouteDecision(fallback);
            context.setModel(fallback.getModel());
            context.setProvider(fallback.getProvider());
            context.setApiKey(fallback.getApiKey());
            precheck.accept(context);
            return invoker.apply(context);
        }
    }

    private void saveLlmCall(RuntimeRunEntity run,
                             RuntimeTraceStepEntity step,
                             ChatRunContext context,
                             LlmCallResult result,
                             boolean success,
                             String errorMessage,
                             BigDecimal cost) {
        RuntimeLlmCallEntity call = new RuntimeLlmCallEntity();
        call.setId(newId());
        call.setRunId(run.getId());
        call.setStepId(step.getId());
        call.setProviderId(context.getProvider().getId());
        call.setModelId(context.getModel().getId());
        if (context.getRouteDecision() != null) {
            call.setRoutePolicyId(context.getRouteDecision().getRoutePolicyId());
            call.setGatewaySceneType(context.getRouteDecision().getSceneType());
            call.setRouteDecision(modelGatewayService.toDecisionJson(context.getRouteDecision()));
            call.setFallbackUsed(Boolean.TRUE.equals(context.getRouteDecision().getFallbackUsed()));
        }
        call.setRequestMessages(toJson(context.getMessages()));
        call.setResponseMessage(toJson(Map.of("content", safeText(result.getContent()))));
        call.setStream(false);
        call.setPromptTokens(nullToZero(result.getPromptTokens()));
        call.setCompletionTokens(nullToZero(result.getCompletionTokens()));
        call.setTotalTokens(nullToZero(result.getTotalTokens()));
        call.setCostAmount(cost == null ? BigDecimal.ZERO : cost);
        call.setLatencyMs(nullToZero(result.getLatencyMs()));
        call.setSuccess(success);
        call.setErrorMessage(errorMessage);
        promptRuntimeService.enrichLlmCall(call, context.getPromptCompileResult(), context.getMessages());
        runtimeLlmCallMapper.insert(call);
        promptRuntimeService.recordMetric(
                context.getAgent() == null ? null : context.getAgent().getWorkspaceId(),
                run.getId(), run.getAgentId(), context.getPromptCompileResult(), success,
                call.getLatencyMs(), call.getTotalTokens(), call.getCostAmount());
        usageCostService.recordActualUsage(run, context.getProvider(), context.getModel(), call.getTotalTokens(), call.getCostAmount(), success, call.getLatencyMs());
    }

    /**
     * 汇总工作流运行下所有 LLM 调用成本。
     *
     * @param runId Runtime 运行 ID
     * @return 成本总额
     */
    private BigDecimal sumWorkflowCost(String runId) {
        return runtimeLlmCallMapper.selectList(new LambdaQueryWrapper<RuntimeLlmCallEntity>()
                        .eq(RuntimeLlmCallEntity::getRunId, runId))
                .stream()
                .map(RuntimeLlmCallEntity::getCostAmount)
                .filter(java.util.Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * 构建 LLM 消息列表。
     */
    private List<ChatMessage> buildMessages(AgentEntity agent,
                                            Map<String, Object> config,
                                            Map<String, Object> context,
                                            PromptRuntimeDtos.CompileResult promptCompileResult) {
        List<ChatMessage> messages = new ArrayList<>();
        String systemPrompt = promptCompileResult != null && StringUtils.hasText(promptCompileResult.renderedPrompt)
                ? promptCompileResult.renderedPrompt
                : stringValue(config.get("systemPrompt"),
                agent != null && StringUtils.hasText(agent.getSystemPrompt())
                        ? agent.getSystemPrompt()
                        : "你是 OpenAgentFlow-Java 工作流中的 LLM 节点，请根据上下文给出清晰、准确的中文回答。");
        messages.add(new ChatMessage("system", renderTemplate(systemPrompt, context)));
        if (promptCompileResult == null) {
            List<KnowledgeSource> sources = sourcesFromContext(context);
            if (!sources.isEmpty()) messages.add(new ChatMessage("system", buildRagPrompt(sources)));
            String workflowContextPrompt = buildWorkflowContextPrompt(context);
            if (StringUtils.hasText(workflowContextPrompt)) messages.add(new ChatMessage("system", workflowContextPrompt));
        }
        String promptTemplate = stringValue(config.get("promptTemplate"), "{{input}}");
        messages.add(new ChatMessage("user", renderTemplate(promptTemplate, context)));
        return messages;
    }

    /**
     * 通过统一Runtime编译工作流LLM节点Prompt，并把RAG、工具和上下文作为可解释装配层。
     */
    private PromptRuntimeDtos.CompileResult compileWorkflowPrompt(AgentEntity agent,
                                                                  Map<String, Object> config,
                                                                  Map<String, Object> context,
                                                                  String runId) {
        PromptRuntimeDtos.CompileRequest request = new PromptRuntimeDtos.CompileRequest();
        request.resourceType = "workflow";
        request.resourceId = stringValue(context.get("workflowId"), "");
        request.agentId = agent == null ? null : agent.getId();
        request.runId = runId;
        request.templateId = stringValue(config.get("promptTemplateId"), agent == null ? "" : agent.getSystemPromptTemplateId());
        request.versionId = stringValue(config.get("promptVersionId"), agent == null ? "" : agent.getSystemPromptVersionId());
        request.bindingMode = stringValue(config.get("promptBindingMode"), agent == null ? "MANUAL" : agent.getPromptBindingMode());
        request.content = stringValue(config.get("systemPrompt"),
                agent != null && StringUtils.hasText(agent.getSystemPrompt()) ? agent.getSystemPrompt()
                        : "你是 OpenAgentFlow-Java 工作流中的 LLM 节点，请根据上下文给出清晰、准确的中文回答。");
        request.variableSchema = stringValue(config.get("variableSchema"), "[]");
        request.variables = new LinkedHashMap<>(context);
        request.variables.put("context", toJson(context));
        request.routingKey = firstText(stringValue(context.get("sessionId"), ""), runId);
        request.strict = false;
        List<KnowledgeSource> sources = sourcesFromContext(context);
        if (!sources.isEmpty()) {
            request.layers.add(new PromptRuntimeDtos.PromptLayer("rag", "RAG证据", buildRagPrompt(sources), 40));
        }
        String workflowContextPrompt = buildWorkflowContextPrompt(context);
        if (StringUtils.hasText(workflowContextPrompt)) {
            request.layers.add(new PromptRuntimeDtos.PromptLayer("workflow", "工作流上下文", workflowContextPrompt, 60));
        }
        if (config.containsKey("safetyPrompt") && StringUtils.hasText(String.valueOf(config.get("safetyPrompt")))) {
            request.layers.add(new PromptRuntimeDtos.PromptLayer("safety", "安全约束", String.valueOf(config.get("safetyPrompt")), 90));
        }
        return promptRuntimeService.compile(request);
    }

    /**
     * 构建工作流结构化上下文提示，让 LLM 节点能明确看到上游工具和节点输出。
     *
     * @param context 工作流上下文
     * @return 上下文提示词
     */
    private String buildWorkflowContextPrompt(Map<String, Object> context) {
        Object toolResult = context.get("toolResult");
        Object lastOutput = context.get("lastOutput");
        if (toolResult == null && !(lastOutput instanceof Map<?, ?>) && !(lastOutput instanceof List<?>)) {
            return "";
        }
        StringBuilder builder = new StringBuilder("以下是工作流上游节点已经产生的可信结构化结果，请作为事实依据使用。\n");
        if (toolResult != null) {
            builder.append("\n[已执行工具结果]\n").append(toJson(toolResult));
            builder.append("\n如果工具结果 success=true 且 responseJson.found=true，必须直接使用工具返回的订单状态、物流单号、预计送达时间和处理建议，不要回答缺少工具结果。");
            builder.append("\n如果 responseJson.queryType=order_summary，必须直接回答 orderCount 和 orders 中的订单摘要，不要转向知识库拒答。");
        }
        if (lastOutput != null && lastOutput != toolResult && !(lastOutput instanceof String)) {
            builder.append("\n[上一节点输出]\n").append(toJson(lastOutput));
        }
        builder.append("\n若 RAG 来源为空，但工具结果已成功返回，仍可基于工具结果回答工具覆盖的事实；涉及退款、赔付等高风险动作，只给出处理建议并提示人工确认。");
        return builder.toString();
    }

    /**
     * 构建 RAG 引用提示词。
     */
    private String buildRagPrompt(List<KnowledgeSource> sources) {
        StringBuilder builder = new StringBuilder("以下是 RAG 节点检索到的参考来源，请优先依据来源回答，并在需要时说明依据。\n");
        for (int index = 0; index < sources.size(); index++) {
            KnowledgeSource source = sources.get(index);
            builder.append("\n[来源").append(index + 1).append("] ")
                    .append(source.getDocumentName()).append(" / 分片 ").append(source.getChunkNo())
                    .append("\n").append(source.getQuoteText());
        }
        return builder.toString();
    }

    /**
     * 解析模型。
     */
    private ModelConfigEntity resolveModel(Map<String, Object> config, AgentEntity agent) {
        String modelId = stringValue(config.get("modelId"), agent == null ? "" : agent.getModelId());
        if (StringUtils.hasText(modelId)) {
            return modelProviderService.requireModel(modelId);
        }
        return modelConfigMapper.selectList(new LambdaQueryWrapper<ModelConfigEntity>()
                        .eq(ModelConfigEntity::getModelType, "chat")
                        .eq(ModelConfigEntity::getStatus, "enabled")
                        .orderByDesc(ModelConfigEntity::getIsDefault)
                        .orderByDesc(ModelConfigEntity::getCreatedAt))
                .stream()
                .findFirst()
                .orElseThrow(() -> new BusinessException("MODEL_NOT_FOUND", "请先配置可用的 Chat 模型"));
    }

    /**
     * 解析 Agent。
     */
    private AgentEntity resolveAgent(String agentId) {
        if (!StringUtils.hasText(agentId)) {
            return null;
        }
        AgentEntity agent = agentMapper.selectById(agentId);
        if (agent == null || agent.getDeletedAt() != null) {
            throw new BusinessException("AGENT_NOT_FOUND", "Agent 不存在");
        }
        agentAccessService.assertCanView(agent);
        return agent;
    }

    /**
     * 查找当前发布版本。
     */
    private WorkflowVersionEntity resolveVersion(WorkflowDefinitionEntity workflow, WorkflowDtos.RunRequest request) {
        List<WorkflowVersionEntity> versions = workflowVersionMapper.selectList(new LambdaQueryWrapper<WorkflowVersionEntity>()
                .eq(WorkflowVersionEntity::getWorkflowId, workflow.getId())
                .eq(WorkflowVersionEntity::getStatus, "published")
                .orderByDesc(WorkflowVersionEntity::getCreatedAt));
        if (versions.isEmpty()) {
            return null;
        }
        WorkflowVersionEntity current = versions.stream()
                .filter(item -> item.getVersionNo().equals(workflow.getPublishedVersion()))
                .findFirst().orElse(versions.getFirst());
        Map<String, Object> policy = parseMap(parseMap(workflow.getGraphJson()).get("executionPolicy"));
        String strategy = stringValue(policy.get("releaseStrategy"), "standard");
        int grayPercent = numberValue(policy.get("grayPercent"), 100D).intValue();
        if (!"gray".equalsIgnoreCase(strategy) || versions.size() < 2) {
            return current;
        }
        String requestKey = request == null ? "" : firstText(request.getIdempotencyKey(), safeText(request.getInput()));
        boolean useCurrent = WorkflowGrayReleasePolicy.useCurrentVersion(workflow.getId(),
                agentAccessService.currentUserId(), requestKey, grayPercent);
        return useCurrent ? current : versions.stream().filter(item -> !item.getId().equals(current.getId()))
                .findFirst().orElse(current);
    }

    /**
     * 查询启用节点。
     */
    private List<WorkflowNodeEntity> listNodes(String workflowId, WorkflowVersionEntity version) {
        List<Map<String, Object>> snapshot = graphItems(version == null ? null : version.getGraphJson(), "nodes");
        if (!snapshot.isEmpty()) {
            return snapshot.stream().map(item -> snapshotNode(workflowId, item)).filter(WorkflowNodeEntity::getEnabled).toList();
        }
        return workflowNodeMapper.selectList(new LambdaQueryWrapper<WorkflowNodeEntity>()
                        .eq(WorkflowNodeEntity::getWorkflowId, workflowId)
                        .eq(WorkflowNodeEntity::getEnabled, true)
                        .orderByAsc(WorkflowNodeEntity::getCreatedAt))
                .stream()
                .sorted(Comparator.comparing(item -> item.getCreatedAt() == null ? LocalDateTime.MIN : item.getCreatedAt()))
                .toList();
    }

    /**
     * 查询连线。
     */
    private List<WorkflowEdgeEntity> listEdges(String workflowId, WorkflowVersionEntity version) {
        List<Map<String, Object>> snapshot = graphItems(version == null ? null : version.getGraphJson(), "edges");
        if (!snapshot.isEmpty()) {
            return snapshot.stream().map(item -> snapshotEdge(workflowId, item)).toList();
        }
        return workflowEdgeMapper.selectList(new LambdaQueryWrapper<WorkflowEdgeEntity>()
                        .eq(WorkflowEdgeEntity::getWorkflowId, workflowId)
                        .orderByAsc(WorkflowEdgeEntity::getCreatedAt))
                .stream()
                .sorted(Comparator.comparing(item -> item.getCreatedAt() == null ? LocalDateTime.MIN : item.getCreatedAt()))
                .toList();
    }

    /** 从版本画布中读取节点或连线数组。 */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> graphItems(String graphJson, String key) {
        Object value = parseMap(graphJson).get(key);
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().filter(Map.class::isInstance)
                .<Map<String, Object>>map(item -> new LinkedHashMap<>((Map<String, Object>) item)).toList();
    }

    /** 将版本快照节点转换为执行实体。 */
    private WorkflowNodeEntity snapshotNode(String workflowId, Map<String, Object> item) {
        WorkflowNodeEntity node = new WorkflowNodeEntity();
        node.setId(newId());
        node.setWorkflowId(workflowId);
        node.setNodeKey(stringValue(firstNonNull(item.get("nodeKey"), item.get("node_key"), item.get("id")), newId()));
        node.setNodeName(stringValue(firstNonNull(item.get("nodeName"), item.get("node_name"), item.get("label")), node.getNodeKey()));
        node.setNodeType(stringValue(firstNonNull(item.get("nodeType"), item.get("node_type"), item.get("type")), "LLM").toUpperCase(Locale.ROOT));
        node.setConfigJson(toJson(firstNonNull(item.get("configJson"), item.get("config_json"), item.get("data"), Map.of())));
        node.setInputSchema(toJson(firstNonNull(item.get("inputSchema"), item.get("input_schema"), Map.of())));
        node.setOutputSchema(toJson(firstNonNull(item.get("outputSchema"), item.get("output_schema"), Map.of())));
        node.setRetryPolicy(toJson(firstNonNull(item.get("retryPolicy"), item.get("retry_policy"), Map.of())));
        node.setEnabled(booleanValue(item.get("enabled"), true));
        return node;
    }

    /** 将版本快照连线转换为执行实体。 */
    private WorkflowEdgeEntity snapshotEdge(String workflowId, Map<String, Object> item) {
        WorkflowEdgeEntity edge = new WorkflowEdgeEntity();
        edge.setId(newId());
        edge.setWorkflowId(workflowId);
        edge.setEdgeKey(stringValue(firstNonNull(item.get("edgeKey"), item.get("edge_key"), item.get("id")), newId()));
        edge.setSourceNodeKey(stringValue(firstNonNull(item.get("sourceNodeKey"), item.get("source_node_key"), item.get("source")), ""));
        edge.setTargetNodeKey(stringValue(firstNonNull(item.get("targetNodeKey"), item.get("target_node_key"), item.get("target")), ""));
        edge.setConditionExpr(stringValue(firstNonNull(item.get("conditionExpr"), item.get("condition_expr")), null));
        edge.setLabel(stringValue(item.get("label"), null));
        edge.setMetadata(toJson(firstNonNull(item.get("metadata"), Map.of())));
        return edge;
    }

    /**
     * 查找开始节点。
     */
    private WorkflowNodeEntity findStartNode(List<WorkflowNodeEntity> nodes) {
        return findStartNode(nodes, null);
    }

    /**
     * 查找起始节点，调试模式允许从指定节点开始。
     */
    private WorkflowNodeEntity findStartNode(List<WorkflowNodeEntity> nodes, String startNodeKey) {
        if (StringUtils.hasText(startNodeKey)) {
            return nodes.stream()
                    .filter(node -> startNodeKey.equals(node.getNodeKey()))
                    .findFirst()
                    .orElseThrow(() -> new BusinessException("WORKFLOW_START_NODE_NOT_FOUND", "调试起始节点不存在"));
        }
        return nodes.stream()
                .filter(node -> "START".equalsIgnoreCase(node.getNodeType()))
                .findFirst()
                .orElse(nodes.get(0));
    }

    /**
     * 判断条件表达式是否命中。
     */
    private boolean matches(String expression, Map<String, Object> context) {
        String expr = safeText(expression).trim();
        if (!StringUtils.hasText(expr) || "always".equalsIgnoreCase(expr) || "default".equalsIgnoreCase(expr)) {
            return true;
        }
        if (expr.toLowerCase(Locale.ROOT).startsWith("intent:")) {
            // 意图条件调用统一领域策略，避免工作流节点继续维护易碎的整句关键词列表。
            String intentCode = expr.substring("intent:".length()).trim();
            return OrderQueryIntentPolicy.matchesIntent(intentCode, stringValue(context.get("input"), ""));
        }
        if (expr.contains("&&")) {
            for (String part : expr.split("&&")) {
                if (!matches(part.trim(), context)) {
                    return false;
                }
            }
            return true;
        }
        if (expr.contains("||")) {
            for (String part : expr.split("\\|\\|")) {
                if (matches(part.trim(), context)) {
                    return true;
                }
            }
            return false;
        }
        if ("success".equalsIgnoreCase(expr)) {
            return !context.containsKey("error");
        }
        String lastOutput = String.valueOf(context.getOrDefault("lastOutput", ""));
        if (expr.toLowerCase(Locale.ROOT).startsWith("contains:")) {
            return lastOutput.contains(expr.substring("contains:".length()));
        }
        if (expr.toLowerCase(Locale.ROOT).startsWith("equals:")) {
            return lastOutput.equals(expr.substring("equals:".length()));
        }
        if (expr.toLowerCase(Locale.ROOT).startsWith("json:")) {
            String path = expr.substring("json:".length());
            return pathValue(context, path) != null;
        }
        String lowerExpr = expr.toLowerCase(Locale.ROOT);
        int notContainsIndex = lowerExpr.indexOf(" not contains ");
        if (notContainsIndex > 0) {
            Object left = pathValue(context, expr.substring(0, notContainsIndex).trim());
            String right = unquote(expr.substring(notContainsIndex + " not contains ".length()).trim());
            return left == null || !String.valueOf(left).contains(right);
        }
        int containsIndex = lowerExpr.indexOf(" contains ");
        if (containsIndex > 0) {
            Object left = pathValue(context, expr.substring(0, containsIndex).trim());
            String right = unquote(expr.substring(containsIndex + " contains ".length()).trim());
            return left != null && String.valueOf(left).contains(right);
        }
        for (String operator : List.of(">=", "<=", "==", "!=", ">", "<")) {
            int index = expr.indexOf(operator);
            if (index > 0) {
                Object left = pathValue(context, expr.substring(0, index).trim());
                String right = unquote(expr.substring(index + operator.length()).trim());
                return compare(left, right, operator);
            }
        }
        if ("true".equalsIgnoreCase(expr) || "false".equalsIgnoreCase(expr)) {
            return Boolean.parseBoolean(expr);
        }
        return Boolean.TRUE.equals(context.get("conditionMatched"));
    }

    /**
     * 渲染字符串模板。
     */
    private String renderTemplate(String template, Map<String, Object> context) {
        Matcher matcher = TEMPLATE_PATTERN.matcher(template == null ? "" : template);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            Object value = pathValue(context, matcher.group(1));
            if (value == null) {
                value = "";
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(value instanceof String ? (String) value : toJson(value)));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    /**
     * 渲染参数 Map。
     */
    private Map<String, Object> renderMap(Map<String, Object> source, Map<String, Object> context) {
        Map<String, Object> rendered = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (value instanceof String text) {
                rendered.put(key, renderTemplate(text, context));
            } else if (value instanceof Map<?, ?> map) {
                rendered.put(key, renderMap(parseMap(map), context));
            } else if (value instanceof List<?> list) {
                rendered.put(key, list.stream()
                        .map(item -> item instanceof String text ? renderTemplate(text, context) : item)
                        .toList());
            } else {
                rendered.put(key, value);
            }
        });
        return rendered;
    }

    /**
     * 从上下文中读取 RAG 来源。
     */
    @SuppressWarnings("unchecked")
    private List<KnowledgeSource> sourcesFromContext(Map<String, Object> context) {
        Object sources = context.get("sources");
        if (sources instanceof List<?> list && (list.isEmpty() || list.get(0) instanceof KnowledgeSource)) {
            return (List<KnowledgeSource>) list;
        }
        return List.of();
    }

    /**
     * 转换运行结果。
     */
    private WorkflowDtos.RunResult toRunResult(WorkflowRunEntity workflowRun,
                                               RuntimeRunEntity runtimeRun,
                                               Map<String, Object> context,
                                               String output,
                                               List<WorkflowDtos.StepResult> steps,
                                               int totalTokens,
                                               String errorMessage) {
        WorkflowDtos.RunResult result = new WorkflowDtos.RunResult();
        result.setWorkflowRunId(workflowRun.getId());
        result.setRuntimeRunId(runtimeRun.getId());
        result.setWorkflowId(workflowRun.getWorkflowId());
        result.setWorkflowVersionId(workflowRun.getWorkflowVersionId());
        result.setAgentId(workflowRun.getAgentId());
        result.setTriggerType(workflowRun.getTriggerType());
        result.setStatus(StringUtils.hasText(workflowRun.getStatus()) ? workflowRun.getStatus() : (errorMessage == null ? "SUCCESS" : "FAILED"));
        result.setOutputText(safeText(output));
        result.setContext(context);
        result.setSteps(steps);
        result.setTotalTokens(totalTokens);
        result.setLatencyMs(runtimeRun.getLatencyMs());
        result.setErrorMessage(errorMessage);
        result.setIdempotencyKey(workflowRun.getIdempotencyKey());
        result.setParentRunId(workflowRun.getParentRunId());
        result.setResumeFromNodeKey(workflowRun.getResumeFromNodeKey());
        result.setLastNodeKey(workflowRun.getLastNodeKey());
        result.setNextNodeKey(workflowRun.getNextNodeKey());
        result.setRecoverable(workflowRun.getRecoverable());
        result.setRetryCount(nullToZero(workflowRun.getRetryCount()));
        result.setStartedAt(workflowRun.getStartedAt());
        result.setFinishedAt(workflowRun.getFinishedAt());
        return result;
    }

    /**
     * 解析 JSON Map。
     */
    private Map<String, Object> parseMap(String json) {
        try {
            if (!StringUtils.hasText(json)) {
                return new LinkedHashMap<>();
            }
            return new LinkedHashMap<>(objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
            }));
        } catch (Exception exception) {
            return new LinkedHashMap<>();
        }
    }

    /**
     * 解析对象 Map。
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(String.valueOf(key), item));
            return result;
        }
        if (value instanceof String text) {
            return parseMap(text);
        }
        return new LinkedHashMap<>();
    }

    /**
     * 读取字符串配置。
     */
    /**
     * 根据点路径读取上下文值，支持 a.b[0] 这类表达式。
     */
    private Object pathValue(Map<String, Object> context, String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        if (context.containsKey(path)) {
            return context.get(path);
        }
        Object current = context;
        for (String rawPart : path.split("\\.")) {
            String part = rawPart.trim();
            if (!StringUtils.hasText(part)) {
                continue;
            }
            int arrayIndex = -1;
            if (part.contains("[") && part.endsWith("]")) {
                int start = part.indexOf('[');
                arrayIndex = numberValue(part.substring(start + 1, part.length() - 1), -1D).intValue();
                part = part.substring(0, start);
            }
            if (current instanceof Map<?, ?> map) {
                current = map.get(part);
            } else {
                return null;
            }
            if (arrayIndex >= 0) {
                if (current instanceof List<?> list && arrayIndex < list.size()) {
                    current = list.get(arrayIndex);
                } else {
                    return null;
                }
            }
        }
        return current;
    }

    /**
     * 比较条件表达式两侧的值。
     */
    private boolean compare(Object left, String right, String operator) {
        if (left == null) {
            return "!=".equals(operator);
        }
        try {
            double leftNumber = Double.parseDouble(String.valueOf(left));
            double rightNumber = Double.parseDouble(right);
            return switch (operator) {
                case ">" -> leftNumber > rightNumber;
                case ">=" -> leftNumber >= rightNumber;
                case "<" -> leftNumber < rightNumber;
                case "<=" -> leftNumber <= rightNumber;
                case "==" -> leftNumber == rightNumber;
                case "!=" -> leftNumber != rightNumber;
                default -> false;
            };
        } catch (Exception ignored) {
            return switch (operator) {
                case "==" -> String.valueOf(left).equals(right);
                case "!=" -> !String.valueOf(left).equals(right);
                default -> false;
            };
        }
    }

    /**
     * 检查工作流或节点预算，超过预算时阻断运行。
     */
    private void enforceBudget(WorkflowDefinitionEntity workflow,
                               RuntimeRunEntity runtimeRun,
                               WorkflowNodeEntity node,
                               Map<String, Object> context,
                               int totalTokens) {
        Map<String, Object> nodeConfig = parseMap(node.getConfigJson());
        Map<String, Object> graph = parseMap(workflow.getGraphJson());
        Map<String, Object> workflowPolicy = parseMap(graph.get("executionPolicy"));
        int budgetTokens = numberValue(firstNonNull(nodeConfig.get("budgetTokens"), workflowPolicy.get("budgetTokens")), 0D).intValue();
        if (budgetTokens > 0 && totalTokens > budgetTokens) {
            writePolicyHit(runtimeRun, node, "budget", "block", Map.of("budgetTokens", budgetTokens, "actualTokens", totalTokens), "工作流 Token 超过预算");
            throw new BusinessException("WORKFLOW_BUDGET_EXCEEDED", "工作流 Token 超过预算");
        }
        String sandboxLevel = stringValue(firstNonNull(nodeConfig.get("sandboxLevel"), workflowPolicy.get("sandboxLevel")), "low");
        if ("high".equalsIgnoreCase(sandboxLevel) && "TOOL".equalsIgnoreCase(node.getNodeType()) && !Boolean.TRUE.equals(nodeConfig.get("humanConfirmed"))) {
            writePolicyHit(runtimeRun, node, "sandbox", "warn", nodeConfig, "高风险工具节点建议增加人工确认");
            context.put("sandboxWarning", "高风险工具节点建议增加人工确认");
        }
    }

    /**
     * 写入工作流策略命中日志。
     */
    private void writePolicyHit(RuntimeRunEntity run,
                                WorkflowNodeEntity node,
                                String policyType,
                                String hitResult,
                                Object policySnapshot,
                                String message) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO workflow_policy_hit_log
                      (id, workflow_id, workflow_run_id, node_key, policy_type, hit_result, policy_snapshot, message)
                    VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?)
                    """,
                    newId(),
                    run.getWorkflowId(),
                    run.getWorkflowRunId(),
                    node.getNodeKey(),
                    policyType,
                    hitResult,
                    toJson(policySnapshot),
                    message);
        } catch (Exception ignored) {
            // 策略日志不能影响主流程执行。
        }
    }

    /**
     * 返回第一个非空值。
     */
    private Object firstNonNull(Object... values) {
        if (values == null) {
            return null;
        }
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * 安静等待重试间隔。
     */
    private void sleepQuietly(int millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(Math.min(millis, 5000));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 判断节点是否会触发真实外部调用。
     */
    private boolean isExternalNode(String nodeType) {
        return List.of("LLM", "RAG", "TOOL", "PLUGIN", "API", "WEBHOOK", "NOTIFY").contains(safeText(nodeType).toUpperCase(Locale.ROOT));
    }

    /**
     * 判断节点是否为工作流终止节点。
     */
    private boolean isTerminalNode(String nodeType) {
        return List.of("END", "OUTPUT").contains(safeText(nodeType).toUpperCase(Locale.ROOT));
    }

    private String stringValue(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    /**
     * 读取数字配置。
     */
    private Double numberValue(Object value, Double fallback) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return value == null ? fallback : Double.parseDouble(String.valueOf(value));
        } catch (Exception exception) {
            return fallback;
        }
    }

    /**
     * 读取布尔配置，兼容前端传入的字符串值。
     */
    private boolean booleanValue(Object value, boolean fallback) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value == null) {
            return fallback;
        }
        String text = String.valueOf(value).trim();
        if (!StringUtils.hasText(text)) {
            return fallback;
        }
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "yes".equalsIgnoreCase(text);
    }

    /**
     * 去除条件表达式右侧的单双引号。
     */
    private String unquote(String value) {
        String text = safeText(value).trim();
        if ((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("'") && text.endsWith("'"))) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    /**
     * 空字符串兜底。
     */
    private String safeText(String text) {
        return text == null ? "" : text;
    }

    /**
     * 空整数转 0。
     */
    private Integer nullToZero(Integer value) {
        return value == null ? 0 : value;
    }

    /**
     * 转换 JSON 字符串。
     */
    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            return "{}";
        }
    }

    /**
     * 解析任意 JSON 值，解析失败时返回原文本。
     */
    private Object parseJsonValue(String json) {
        try {
            if (!StringUtils.hasText(json)) {
                return null;
            }
            return objectMapper.readValue(json, Object.class);
        } catch (Exception exception) {
            return json;
        }
    }

    /**
     * 返回第一个有文本的字符串。
     */
    private String firstText(String first, String second) {
        return StringUtils.hasText(first) ? first : second;
    }

    /**
     * 生成 UUID。
     */
    private String newId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 节点执行的内部结果。
     */
    /** 并行节点整体执行结果。 */
    private record ParallelExecutionResult(Map<String, Object> context,
                                           Object output,
                                           WorkflowNodeEntity joinNode,
                                           List<WorkflowDtos.StepResult> steps,
                                           int promptTokens,
                                           int completionTokens,
                                           int totalTokens) {
    }

    /** 单条并行分支执行结果。 */
    private record BranchExecutionResult(Map<String, Object> context,
                                         Object output,
                                         List<WorkflowDtos.StepResult> steps,
                                         int promptTokens,
                                         int completionTokens,
                                         int totalTokens) {
    }

    private record NodeConditionDecision(boolean shouldRun,
                                         String expression,
                                         String mode,
                                         boolean matched,
                                         String reason) {
        /**
         * 转换为前端和 Trace 可读的跳过输出。
         */
        private Map<String, Object> toOutput() {
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("skipped", true);
            output.put("expression", safeRecordText(expression));
            output.put("mode", safeRecordText(mode));
            output.put("matched", matched);
            output.put("reason", safeRecordText(reason));
            return output;
        }

        /**
         * record 内部不能直接调用外部实例方法，这里做本地空值兜底。
         */
        private static String safeRecordText(String text) {
            return text == null ? "" : text;
        }
    }

    /**
     * 节点执行的内部结果。
     */
    private record NodeExecutionResult(Object output,
                                       WorkflowDtos.StepResult stepResult,
                                       String status,
                                       String errorMessage,
                                       int promptTokens,
                                       int completionTokens,
                                       int totalTokens,
                                       BigDecimal costAmount) {
        /**
         * 构造成功结果。
         */
        private static NodeExecutionResult success(WorkflowNodeEntity node,
                                                   Object output,
                                                   int promptTokens,
                                                   int completionTokens,
                                                   int totalTokens) {
            return success(node, output, promptTokens, completionTokens, totalTokens, BigDecimal.ZERO);
        }

        /**
         * 构造带成本的成功结果。
         */
        private static NodeExecutionResult success(WorkflowNodeEntity node,
                                                   Object output,
                                                   int promptTokens,
                                                   int completionTokens,
                                                   int totalTokens,
                                                   BigDecimal costAmount) {
            WorkflowDtos.StepResult step = new WorkflowDtos.StepResult();
            step.setNodeKey(node.getNodeKey());
            step.setNodeName(node.getNodeName());
            step.setNodeType(node.getNodeType());
            step.setStatus("SUCCESS");
            step.setOutput(output);
            step.setTokenCount(totalTokens);
            return new NodeExecutionResult(output, step, "SUCCESS", null, promptTokens, completionTokens, totalTokens, costAmount == null ? BigDecimal.ZERO : costAmount);
        }

        /**
         * 构造被节点执行条件跳过的结果。
         */
        private static NodeExecutionResult skipped(WorkflowNodeEntity node, Object output) {
            WorkflowDtos.StepResult step = new WorkflowDtos.StepResult();
            step.setNodeKey(node.getNodeKey());
            step.setNodeName(node.getNodeName());
            step.setNodeType(node.getNodeType());
            step.setStatus("SKIPPED");
            step.setOutput(output);
            step.setTokenCount(0);
            return new NodeExecutionResult(output, step, "SKIPPED", null, 0, 0, 0, BigDecimal.ZERO);
        }

        /**
         * 构造等待人工确认的结果。
         */
        private static NodeExecutionResult waiting(WorkflowNodeEntity node, Object output) {
            WorkflowDtos.StepResult step = new WorkflowDtos.StepResult();
            step.setNodeKey(node.getNodeKey());
            step.setNodeName(node.getNodeName());
            step.setNodeType(node.getNodeType());
            step.setStatus("WAITING");
            step.setOutput(output);
            step.setTokenCount(0);
            return new NodeExecutionResult(output, step, "WAITING", null, 0, 0, 0, BigDecimal.ZERO);
        }

        /**
         * 构造失败但被策略接管的结果。
         */
        private static NodeExecutionResult failure(WorkflowNodeEntity node, Object output, String errorMessage, String status) {
            WorkflowDtos.StepResult step = new WorkflowDtos.StepResult();
            step.setNodeKey(node.getNodeKey());
            step.setNodeName(node.getNodeName());
            step.setNodeType(node.getNodeType());
            step.setStatus(status);
            step.setOutput(output);
            step.setTokenCount(0);
            step.setErrorMessage(errorMessage);
            return new NodeExecutionResult(output, step, status, errorMessage, 0, 0, 0, BigDecimal.ZERO);
        }
    }
}
