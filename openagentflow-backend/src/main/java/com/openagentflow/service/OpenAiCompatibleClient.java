package com.openagentflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openagentflow.domain.chat.ChatMessage;
import com.openagentflow.domain.chat.ChatRunContext;
import com.openagentflow.domain.chat.LlmCallResult;
import com.openagentflow.domain.chat.ToolCallRequest;
import com.openagentflow.domain.chat.ToolDefinitionForModel;
import com.openagentflow.entity.ModelConfigEntity;
import com.openagentflow.entity.ModelProviderEntity;
import com.openagentflow.exception.BusinessException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentHashMap;
import java.io.Closeable;
import java.util.function.Consumer;

/**
 * OpenAI-compatible 模型调用客户端。
 */
@Service
public class OpenAiCompatibleClient {

    /** 运行ID到活动HTTP请求的映射，用于用户停止时主动中断阻塞模型调用。 */
    private final Map<String, CompletableFuture<?>> activeRequests = new ConcurrentHashMap<>();

    /** 运行ID到活动流的映射，用于立即关闭SSE模型响应。 */
    private final Map<String, Closeable> activeStreams = new ConcurrentHashMap<>();

    /** 豆包多模态 Embedding 单请求只返回单条向量，使用小并发降低大文档等待时间并避免过度触发限流。 */
    private static final int MULTIMODAL_EMBEDDING_CONCURRENCY = 4;

    /** HTTP 客户端，复用模型连通性测试、聊天补全和 Embedding 请求。 */
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .build();

    /** JSON 序列化工具。 */
    private final ObjectMapper objectMapper;

    public OpenAiCompatibleClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 普通非流式聊天补全。
     *
     * @param context 聊天运行上下文
     * @param temperature 温度参数
     * @param maxTokens 最大输出 Token 数
     * @return LLM 调用结果
     */
    public LlmCallResult complete(ChatRunContext context, Double temperature, Integer maxTokens) {
        Instant startedAt = Instant.now();
        ObjectNode payload = buildPayload(context, false, temperature, maxTokens);
        try {
            HttpRequest request = buildRequest(context, payload);
            HttpResponse<String> response = sendCancelable(context, request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int latencyMs = (int) Duration.between(startedAt, Instant.now()).toMillis();
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new BusinessException("MODEL_CALL_FAILED", response.body());
            }
            LlmCallResult result = parseNormalResponse(response.body());
            result.setLatencyMs(latencyMs);
            result.setRawResponse(response.body());
            return result;
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new BusinessException("MODEL_CALL_FAILED", exception.getMessage());
        }
    }

    /**
     * 携带工具定义执行非流式聊天补全。
     *
     * @param context 聊天运行上下文
     * @param temperature 温度参数
     * @param maxTokens 最大输出 Token 数
     * @return LLM 调用结果，可能包含 tool_calls
     */
    public LlmCallResult completeWithTools(ChatRunContext context, Double temperature, Integer maxTokens) {
        Instant startedAt = Instant.now();
        ObjectNode payload = buildPayload(context, false, temperature, maxTokens, context.getTools());
        try {
            HttpRequest request = buildRequest(context, payload);
            HttpResponse<String> response = sendCancelable(context, request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int latencyMs = (int) Duration.between(startedAt, Instant.now()).toMillis();
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new BusinessException("MODEL_CALL_FAILED", response.body());
            }
            LlmCallResult result = parseNormalResponse(response.body());
            result.setLatencyMs(latencyMs);
            result.setRawResponse(response.body());
            return result;
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new BusinessException("MODEL_CALL_FAILED", exception.getMessage());
        }
    }

    /**
     * 流式聊天补全。
     *
     * @param context 聊天运行上下文
     * @param temperature 温度参数
     * @param maxTokens 最大输出 Token 数
     * @param onDelta 流式片段回调
     * @return LLM 调用结果
     */
    public LlmCallResult completeStream(ChatRunContext context,
                                        Double temperature,
                                        Integer maxTokens,
                                        Consumer<String> onDelta) {
        Instant startedAt = Instant.now();
        StringBuilder contentBuilder = new StringBuilder();
        StringBuilder rawBuilder = new StringBuilder();
        ObjectNode payload = buildPayload(context, true, temperature, maxTokens);
        LlmCallResult result = new LlmCallResult();
        try {
            HttpRequest request = buildRequest(context, payload);
            HttpResponse<java.io.InputStream> response = sendCancelable(context, request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                String errorBody = new String(response.body().readAllBytes(), StandardCharsets.UTF_8);
                throw new BusinessException("MODEL_CALL_FAILED", errorBody);
            }

            // 按 SSE 行解析模型流式响应，逐段把 delta.content 推给前端。
            registerStream(context, response.body());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("data:")) {
                        continue;
                    }
                    String data = line.substring(5).trim();
                    if ("[DONE]".equals(data)) {
                        break;
                    }
                    rawBuilder.append(data).append('\n');
                    String delta = parseStreamDelta(data, result);
                    if (StringUtils.hasText(delta)) {
                        contentBuilder.append(delta);
                        onDelta.accept(delta);
                    }
                }
            }
            unregisterStream(context);
            result.setContent(contentBuilder.toString());
            result.setRawResponse(rawBuilder.toString());
            result.setLatencyMs((int) Duration.between(startedAt, Instant.now()).toMillis());
            return result;
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new BusinessException("MODEL_STREAM_FAILED", exception.getMessage());
        } finally {
            unregisterStream(context);
        }
    }

    /**
     * 主动取消指定Runtime运行关联的HTTP请求和响应流。
     *
     * @param runId 运行ID
     * @return 是否找到活动调用
     */
    public boolean cancel(String runId) {
        boolean found = false;
        CompletableFuture<?> request = activeRequests.remove(runId);
        if (request != null) {
            found = request.cancel(true);
        }
        Closeable stream = activeStreams.remove(runId);
        if (stream != null) {
            try { stream.close(); } catch (Exception ignored) { }
            found = true;
        }
        return found;
    }

    /** 返回当前JVM正在调用模型的运行ID快照。 */
    public java.util.Set<String> activeRunIds() {
        java.util.Set<String> result = new java.util.HashSet<>(activeRequests.keySet());
        result.addAll(activeStreams.keySet());
        return java.util.Set.copyOf(result);
    }

    /** 使用可取消异步请求替代阻塞send。 */
    private <T> HttpResponse<T> sendCancelable(ChatRunContext context,
                                               HttpRequest request,
                                               HttpResponse.BodyHandler<T> bodyHandler) throws Exception {
        CompletableFuture<HttpResponse<T>> future = httpClient.sendAsync(request, bodyHandler);
        String runId = context == null ? null : context.getRunId();
        if (StringUtils.hasText(runId)) activeRequests.put(runId, future);
        try {
            return future.get();
        } catch (InterruptedException exception) {
            // 工作流截止时间中断等待线程时，同时取消实际 HTTP 请求，不能只结束本地等待。
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw exception;
        } finally {
            if (StringUtils.hasText(runId)) activeRequests.remove(runId, future);
        }
    }

    /** 登记活动模型响应流。 */
    private void registerStream(ChatRunContext context, Closeable stream) {
        if (context != null && StringUtils.hasText(context.getRunId())) activeStreams.put(context.getRunId(), stream);
    }

    /** 清理活动模型响应流。 */
    private void unregisterStream(ChatRunContext context) {
        if (context != null && StringUtils.hasText(context.getRunId())) activeStreams.remove(context.getRunId());
    }

    /**
     * 调用 Embedding 接口生成向量。
     *
     * @param provider 模型服务商
     * @param model Embedding 模型配置
     * @param apiKey API Key 明文
     * @param inputs 待向量化文本列表
     * @return 按输入顺序返回的向量列表
     */
    public List<List<Double>> embeddings(ModelProviderEntity provider,
                                         ModelConfigEntity model,
                                         String apiKey,
                                         List<String> inputs) {
        if (inputs == null || inputs.isEmpty()) {
            return List.of();
        }
        try {
            if (isMultimodalEmbedding(model)) {
                return requestMultimodalEmbeddingsConcurrent(provider, model, apiKey, inputs);
            }
            ObjectNode payload = buildTextEmbeddingPayload(model, inputs);
            HttpRequest request = buildEmbeddingRequest(provider, apiKey, payload, false);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new BusinessException("EMBEDDING_CALL_FAILED", response.body());
            }
            return parseEmbeddingResponse(response.body());
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new BusinessException("EMBEDDING_CALL_FAILED", exception.getMessage());
        }
    }

    /**
     * 调用豆包多模态 Embedding 接口。
     *
     * @param provider 模型服务商
     * @param model Embedding 模型配置
     * @param apiKey API Key 明文
     * @param inputs 待向量化文本列表
     * @return 按输入顺序返回的向量列表
     */
    private List<List<Double>> requestMultimodalEmbeddings(ModelProviderEntity provider,
                                                           ModelConfigEntity model,
                                                           String apiKey,
                                                           List<String> inputs) throws Exception {
        List<List<Double>> vectors = new ArrayList<>();
        for (String input : inputs) {
            // 多模态接口返回 data.embedding 对象；逐条请求可以保持切片和向量的一一对应关系。
            ObjectNode payload = buildMultimodalEmbeddingPayload(model, input);
            HttpRequest request = buildEmbeddingRequest(provider, apiKey, payload, true);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new BusinessException("EMBEDDING_CALL_FAILED", response.body());
            }
            List<List<Double>> current = parseEmbeddingResponse(response.body());
            if (current.isEmpty()) {
                throw new BusinessException("EMBEDDING_EMPTY", "Embedding 接口未返回向量");
            }
            vectors.add(current.get(0));
        }
        return vectors;
    }

    /**
     * 构建普通文本 Embedding 请求体。
     *
     * @param model Embedding 模型配置
     * @param inputs 待向量化文本列表
     * @return JSON 请求体
     */
    /**
     * 并发调用豆包多模态 Embedding 接口。
     *
     * @param provider 模型服务商
     * @param model Embedding 模型配置
     * @param apiKey API Key 明文
     * @param inputs 待向量化文本列表
     * @return 按输入顺序返回的向量列表
     */
    private List<List<Double>> requestMultimodalEmbeddingsConcurrent(ModelProviderEntity provider,
                                                                      ModelConfigEntity model,
                                                                      String apiKey,
                                                                      List<String> inputs) {
        Semaphore semaphore = new Semaphore(Math.min(MULTIMODAL_EMBEDDING_CONCURRENCY, inputs.size()));
        List<CompletableFuture<List<Double>>> futures = new ArrayList<>(inputs.size());
        for (String input : inputs) {
            // 豆包多模态 embedding 示例是单条内容请求，这里只做受控并发，不把多个分片强行塞进同一个 input。
            futures.add(CompletableFuture.supplyAsync(() -> requestSingleMultimodalEmbedding(provider, model, apiKey, input, semaphore)));
        }

        List<List<Double>> vectors = new ArrayList<>(inputs.size());
        for (CompletableFuture<List<Double>> future : futures) {
            try {
                vectors.add(future.join());
            } catch (CompletionException exception) {
                Throwable cause = exception.getCause() == null ? exception : exception.getCause();
                if (cause instanceof BusinessException businessException) {
                    throw businessException;
                }
                throw new BusinessException("EMBEDDING_CALL_FAILED", cause.getMessage());
            }
        }
        return vectors;
    }

    /**
     * 请求单条豆包多模态 Embedding。
     *
     * @param provider 模型服务商
     * @param model Embedding 模型配置
     * @param apiKey API Key 明文
     * @param input 待向量化文本
     * @param semaphore 并发信号量
     * @return 单条向量
     */
    private List<Double> requestSingleMultimodalEmbedding(ModelProviderEntity provider,
                                                          ModelConfigEntity model,
                                                          String apiKey,
                                                          String input,
                                                          Semaphore semaphore) {
        boolean acquired = false;
        try {
            semaphore.acquire();
            acquired = true;
            ObjectNode payload = buildMultimodalEmbeddingPayload(model, input);
            HttpRequest request = buildEmbeddingRequest(provider, apiKey, payload, true);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new BusinessException("EMBEDDING_CALL_FAILED", response.body());
            }
            List<List<Double>> current = parseEmbeddingResponse(response.body());
            if (current.isEmpty()) {
                throw new BusinessException("EMBEDDING_EMPTY", "Embedding 接口未返回向量");
            }
            return current.get(0);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CompletionException(new BusinessException("EMBEDDING_INTERRUPTED", "Embedding 请求被中断"));
        } catch (BusinessException exception) {
            throw new CompletionException(exception);
        } catch (Exception exception) {
            throw new CompletionException(new BusinessException("EMBEDDING_CALL_FAILED", exception.getMessage()));
        } finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    private ObjectNode buildTextEmbeddingPayload(ModelConfigEntity model, List<String> inputs) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("model", model.getModelCode());
        ArrayNode inputNode = payload.putArray("input");
        inputs.forEach(input -> inputNode.add(input == null ? "" : input));
        return payload;
    }

    /**
     * 构建多模态 Embedding 请求体。
     *
     * @param model Embedding 模型配置
     * @param input 待向量化文本
     * @return JSON 请求体
     */
    private ObjectNode buildMultimodalEmbeddingPayload(ModelConfigEntity model, String input) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("model", model.getModelCode());
        ArrayNode inputNode = payload.putArray("input");
        ObjectNode textNode = inputNode.addObject();
        textNode.put("type", "text");
        textNode.put("text", input == null ? "" : input);
        return payload;
    }

    /**
     * 构建 OpenAI-compatible 聊天请求体。
     *
     * @param context 聊天运行上下文
     * @param stream 是否流式输出
     * @param temperature 温度参数
     * @param maxTokens 最大输出 Token 数
     * @return JSON 请求体
     */
    private ObjectNode buildPayload(ChatRunContext context, boolean stream, Double temperature, Integer maxTokens) {
        return buildPayload(context, stream, temperature, maxTokens, null);
    }

    /**
     * 构建 OpenAI-compatible 聊天请求体。
     *
     * @param context 聊天运行上下文
     * @param stream 是否流式输出
     * @param temperature 温度参数
     * @param maxTokens 最大输出 Token 数
     * @param tools 可用工具定义
     * @return JSON 请求体
     */
    private ObjectNode buildPayload(ChatRunContext context,
                                    boolean stream,
                                    Double temperature,
                                    Integer maxTokens,
                                    List<ToolDefinitionForModel> tools) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("model", context.getModel().getModelCode());
        payload.put("stream", stream);
        if (stream) {
            // 流式响应默认可能不返回 usage，这里请求兼容 OpenAI 的服务在最后一个分片返回真实 Token 用量。
            payload.putObject("stream_options").put("include_usage", true);
        }
        if (temperature != null) {
            payload.put("temperature", temperature);
        }
        if (maxTokens != null && maxTokens > 0) {
            payload.put("max_tokens", maxTokens);
        }
        ArrayNode messagesNode = payload.putArray("messages");
        for (ChatMessage message : context.getMessages()) {
            messagesNode.add(buildMessageNode(message));
        }
        if (tools != null && !tools.isEmpty()) {
            ArrayNode toolsNode = payload.putArray("tools");
            for (ToolDefinitionForModel tool : tools) {
                ObjectNode toolNode = toolsNode.addObject();
                toolNode.put("type", "function");
                ObjectNode functionNode = toolNode.putObject("function");
                functionNode.put("name", tool.getName());
                functionNode.put("description", tool.getDescription() == null ? "" : tool.getDescription());
                functionNode.set("parameters", tool.getParameters() == null ? objectMapper.createObjectNode().put("type", "object") : tool.getParameters());
            }
            payload.put("tool_choice", "auto");
        }
        return payload;
    }

    /**
     * 构建单条模型消息 JSON。
     *
     * @param message 聊天消息
     * @return 消息 JSON 节点
     */
    private ObjectNode buildMessageNode(ChatMessage message) {
        ObjectNode messageNode = objectMapper.createObjectNode();
        messageNode.put("role", message.getRole());
        if ("tool".equals(message.getRole())) {
            messageNode.put("tool_call_id", message.getToolCallId());
            messageNode.put("name", message.getName());
            messageNode.put("content", message.getContent() == null ? "" : message.getContent());
            return messageNode;
        }
        messageNode.put("content", message.getContent() == null ? "" : message.getContent());
        if (message.getToolCalls() != null && !message.getToolCalls().isEmpty()) {
            ArrayNode toolCallsNode = messageNode.putArray("tool_calls");
            for (ToolCallRequest call : message.getToolCalls()) {
                ObjectNode callNode = toolCallsNode.addObject();
                callNode.put("id", call.getId());
                callNode.put("type", "function");
                ObjectNode functionNode = callNode.putObject("function");
                functionNode.put("name", call.getName());
                functionNode.put("arguments", call.getArgumentsJson() == null ? "{}" : call.getArgumentsJson());
            }
        }
        return messageNode;
    }

    /**
     * 构建聊天 HTTP 请求。
     *
     * @param context 聊天运行上下文
     * @param payload JSON 请求体
     * @return HTTP 请求
     */
    private HttpRequest buildRequest(ChatRunContext context, ObjectNode payload) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(chatCompletionUrl(context.getProvider().getBaseUrl())))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8));

        if (!"none".equalsIgnoreCase(context.getProvider().getAuthType()) && StringUtils.hasText(context.getApiKey())) {
            builder.header("Authorization", "Bearer " + context.getApiKey());
        }
        appendDefaultHeaders(builder, context.getProvider().getDefaultHeaders());
        return builder.build();
    }

    /**
     * 构建 Embedding HTTP 请求。
     *
     * @param provider 模型服务商
     * @param apiKey API Key 明文
     * @param payload JSON 请求体
     * @param multimodal 是否使用多模态 Embedding 接口
     * @return HTTP 请求
     */
    private HttpRequest buildEmbeddingRequest(ModelProviderEntity provider,
                                              String apiKey,
                                              ObjectNode payload,
                                              boolean multimodal) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(embeddingUrl(provider.getBaseUrl(), multimodal)))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8));

        if (!"none".equalsIgnoreCase(provider.getAuthType()) && StringUtils.hasText(apiKey)) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        appendDefaultHeaders(builder, provider.getDefaultHeaders());
        return builder.build();
    }

    /**
     * 追加服务商配置的默认请求头。
     *
     * @param builder HTTP 请求构建器
     * @param headersJson 请求头 JSON
     */
    private void appendDefaultHeaders(HttpRequest.Builder builder, String headersJson) throws Exception {
        if (!StringUtils.hasText(headersJson)) {
            return;
        }
        JsonNode headersNode = objectMapper.readTree(headersJson);
        if (!headersNode.isObject()) {
            return;
        }
        for (Map.Entry<String, JsonNode> entry : headersNode.properties()) {
            if (StringUtils.hasText(entry.getKey()) && entry.getValue().isTextual()) {
                builder.header(entry.getKey(), entry.getValue().asText());
            }
        }
    }

    /**
     * 拼接聊天补全 URL。
     *
     * @param baseUrl 服务基础地址
     * @return 聊天补全地址
     */
    private String chatCompletionUrl(String baseUrl) {
        String cleaned = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        if (cleaned.endsWith("/chat/completions")) {
            return cleaned;
        }
        return cleaned + "/chat/completions";
    }

    /**
     * 拼接 Embedding URL。
     *
     * @param baseUrl 服务基础地址
     * @param multimodal 是否使用多模态 Embedding 接口
     * @return Embedding 接口地址
     */
    private String embeddingUrl(String baseUrl, boolean multimodal) {
        String cleaned = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        if (multimodal) {
            if (cleaned.endsWith("/embeddings/multimodal")) {
                return cleaned;
            }
            return cleaned + "/embeddings/multimodal";
        }
        if (cleaned.endsWith("/embeddings")) {
            return cleaned;
        }
        return cleaned + "/embeddings";
    }

    /**
     * 判断当前模型是否走多模态 Embedding 接口。
     *
     * @param model Embedding 模型配置
     * @return true 表示请求 /embeddings/multimodal
     */
    private boolean isMultimodalEmbedding(ModelConfigEntity model) throws Exception {
        if (!StringUtils.hasText(model.getDefaultParams())) {
            return false;
        }
        JsonNode params = objectMapper.readTree(model.getDefaultParams());
        return "multimodal".equalsIgnoreCase(params.path("embeddingApi").asText(""));
    }

    /**
     * 解析普通 JSON 聊天响应。
     *
     * @param body 响应体
     * @return LLM 调用结果
     */
    private LlmCallResult parseNormalResponse(String body) throws Exception {
        JsonNode root = objectMapper.readTree(body);
        LlmCallResult result = new LlmCallResult();
        JsonNode messageNode = root.path("choices").path(0).path("message");
        JsonNode contentNode = messageNode.path("content");
        result.setContent(contentNode.isMissingNode() ? "" : contentNode.asText(""));
        result.setToolCalls(parseToolCalls(messageNode.path("tool_calls")));
        fillUsage(result, root.path("usage"));
        return result;
    }

    /**
     * 解析模型返回的工具调用列表。
     *
     * @param toolCallsNode tool_calls 节点
     * @return 工具调用列表
     */
    private List<ToolCallRequest> parseToolCalls(JsonNode toolCallsNode) {
        if (toolCallsNode == null || !toolCallsNode.isArray()) {
            return List.of();
        }
        List<ToolCallRequest> calls = new ArrayList<>();
        for (JsonNode item : toolCallsNode) {
            ToolCallRequest call = new ToolCallRequest();
            call.setId(item.path("id").asText(""));
            call.setName(item.path("function").path("name").asText(""));
            call.setArgumentsJson(item.path("function").path("arguments").asText("{}"));
            if (StringUtils.hasText(call.getName())) {
                calls.add(call);
            }
        }
        return calls;
    }

    /**
     * 解析 Embedding 响应，兼容普通 data 数组和多模态 data.embedding 对象。
     *
     * @param body 响应体
     * @return 向量列表
     */
    private List<List<Double>> parseEmbeddingResponse(String body) throws Exception {
        JsonNode root = objectMapper.readTree(body);
        JsonNode dataNode = root.path("data");
        List<List<Double>> vectors = new ArrayList<>();
        if (dataNode.isArray()) {
            for (JsonNode item : dataNode) {
                JsonNode embeddingNode = item.path("embedding");
                if (!embeddingNode.isMissingNode() && !embeddingNode.isNull()) {
                    vectors.add(parseEmbeddingVector(embeddingNode));
                }
            }
        } else if (dataNode.isObject() && dataNode.has("embedding")) {
            vectors.add(parseEmbeddingVector(dataNode.path("embedding")));
        }
        if (vectors.isEmpty()) {
            throw new BusinessException("EMBEDDING_EMPTY", "Embedding 接口未返回向量");
        }
        return vectors;
    }

    /**
     * 解析单条向量，兼容一维向量和二维包装。
     *
     * @param embeddingNode 向量 JSON 节点
     * @return 向量数值列表
     */
    private List<Double> parseEmbeddingVector(JsonNode embeddingNode) {
        JsonNode vectorNode = embeddingNode;
        if (embeddingNode.isArray() && embeddingNode.size() > 0 && embeddingNode.get(0).isArray()) {
            vectorNode = embeddingNode.get(0);
        }
        List<Double> vector = new ArrayList<>();
        for (JsonNode value : vectorNode) {
            vector.add(value.asDouble());
        }
        return vector;
    }

    /**
     * 解析流式响应片段。
     *
     * @param data SSE data 内容
     * @param result 累计调用结果
     * @return 本次新增文本
     */
    private String parseStreamDelta(String data, LlmCallResult result) throws Exception {
        JsonNode root = objectMapper.readTree(data);
        fillUsage(result, root.path("usage"));
        JsonNode deltaNode = root.path("choices").path(0).path("delta").path("content");
        if (!deltaNode.isMissingNode() && !deltaNode.isNull()) {
            return deltaNode.asText("");
        }
        return "";
    }

    /**
     * 填充 Token 使用量。
     *
     * @param result LLM 调用结果
     * @param usageNode usage 节点
     */
    private void fillUsage(LlmCallResult result, JsonNode usageNode) {
        if (usageNode == null || usageNode.isMissingNode() || usageNode.isNull()) {
            return;
        }
        result.setPromptTokens(usageNode.path("prompt_tokens").asInt(result.getPromptTokens()));
        result.setCompletionTokens(usageNode.path("completion_tokens").asInt(result.getCompletionTokens()));
        result.setTotalTokens(usageNode.path("total_tokens").asInt(result.getTotalTokens()));
    }
}
