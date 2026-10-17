package com.openagentflow.service;

import com.openagentflow.config.OpenAgentFlowProperties;
import com.openagentflow.entity.KnowledgeChunkEntity;
import com.openagentflow.entity.KnowledgeEmbeddingEntity;
import com.openagentflow.entity.AgentMemoryEntity;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.dml.UpsertParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.SearchResultsWrapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Milvus 知识向量写入服务。
 */
@Service
public class MilvusKnowledgeVectorService {

    /** Milvus 向量检索命中项。 */
    public record VectorHit(String chunkId, String documentId, String kbId, double score) {
    }

    /** Milvus Memory向量检索命中项。 */
    public record MemoryHit(String memoryId, double score) {
    }

    /**
     * 批量幂等写入Memory向量。
     *
     * @param collectionName 集合基础名称
     * @param memories 记忆实体
     * @param vectors 向量值
     */
    public void upsertMemories(String collectionName,
                               List<AgentMemoryEntity> memories,
                               List<List<Double>> vectors) {
        if (memories == null || memories.isEmpty()) return;
        if (vectors == null || memories.size() != vectors.size()) {
            throw new IllegalArgumentException("Memory与向量数量不一致");
        }
        int dimension = vectors.getFirst().size();
        String base = StringUtils.hasText(collectionName) ? collectionName : properties.getMilvus().getDefaultMemoryCollection();
        String target = dimensionCollectionName(base, dimension);
        ensureMemoryCollection(target, dimension);

        List<String> primaryKeys = new ArrayList<>();
        List<String> memoryIds = new ArrayList<>();
        List<String> workspaceIds = new ArrayList<>();
        List<String> agentIds = new ArrayList<>();
        List<String> userIds = new ArrayList<>();
        List<String> privacyScopes = new ArrayList<>();
        List<String> contents = new ArrayList<>();
        List<List<Float>> floatVectors = new ArrayList<>();
        for (int index = 0; index < memories.size(); index++) {
            AgentMemoryEntity memory = memories.get(index);
            List<Double> vector = vectors.get(index);
            if (vector.size() != dimension) throw new IllegalArgumentException("同批Memory向量维度必须一致");
            primaryKeys.add(memory.getVectorPrimaryKey());
            memoryIds.add(memory.getId());
            workspaceIds.add(value(memory.getWorkspaceId()));
            agentIds.add(value(memory.getAgentId()));
            userIds.add(value(memory.getUserId()));
            privacyScopes.add(value(memory.getPrivacyScope()));
            contents.add(truncate(memory.getMemoryText(), 4096));
            floatVectors.add(vector.stream().map(Double::floatValue).toList());
        }
        R<?> result = requireMilvusClient().upsert(UpsertParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(target)
                .withFields(List.of(
                        new InsertParam.Field("vector_primary_key", primaryKeys),
                        new InsertParam.Field("memory_id", memoryIds),
                        new InsertParam.Field("workspace_id", workspaceIds),
                        new InsertParam.Field("agent_id", agentIds),
                        new InsertParam.Field("user_id", userIds),
                        new InsertParam.Field("privacy_scope", privacyScopes),
                        new InsertParam.Field("content", contents),
                        new InsertParam.Field("embedding", floatVectors)))
                .build());
        assertMilvusSuccess(result, "Milvus Memory向量写入失败");
        requireMilvusClient().flush(FlushParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .addCollectionName(target).withSyncFlush(false).build());
    }

    /**
     * 使用ANN和租户标量条件召回Memory。
     */
    public List<MemoryHit> searchMemories(String collectionName,
                                          String workspaceId,
                                          String agentId,
                                          String userId,
                                          List<Double> queryVector,
                                          int topK) {
        if (queryVector == null || queryVector.isEmpty() || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(agentId)) {
            return List.of();
        }
        String base = StringUtils.hasText(collectionName) ? collectionName : properties.getMilvus().getDefaultMemoryCollection();
        String target = dimensionCollectionName(base, queryVector.size());
        ensureMemoryCollection(target, queryVector.size());
        String expression = "workspace_id == \"" + escapeExpressionValue(workspaceId) + "\""
                + " && agent_id == \"" + escapeExpressionValue(agentId) + "\""
                + " && (user_id == \"" + escapeExpressionValue(value(userId)) + "\""
                + " || privacy_scope in [\"agent\",\"workspace\"])";
        R<SearchResults> result = requireMilvusClient().search(SearchParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(target).withMetricType(MetricType.COSINE)
                .withVectorFieldName("embedding").withTopK(Math.max(1, topK))
                .withExpr(expression).withOutFields(List.of("memory_id"))
                .withFloatVectors(List.of(queryVector.stream().map(Double::floatValue).toList()))
                .withParams("{\"ef\":" + Math.max(64, Math.min(512, topK * 4)) + "}").build());
        assertMilvusSuccess(result, "Milvus Memory向量检索失败");
        SearchResultsWrapper wrapper = new SearchResultsWrapper(result.getData().getResults());
        List<MemoryHit> hits = new ArrayList<>();
        for (SearchResultsWrapper.IDScore item : wrapper.getIDScore(0)) {
            hits.add(new MemoryHit(String.valueOf(item.getFieldValues().getOrDefault("memory_id", "")),
                    Math.max(0D, Math.min(1D, item.getScore()))));
        }
        return hits;
    }

    /** 删除单条Memory向量。 */
    public void deleteMemory(String collectionName, int dimension, String vectorPrimaryKey) {
        String base = StringUtils.hasText(collectionName) ? collectionName : properties.getMilvus().getDefaultMemoryCollection();
        String target = dimensionCollectionName(base, dimension);
        R<?> result = requireMilvusClient().delete(DeleteParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName()).withCollectionName(target)
                .withExpr("vector_primary_key == \"" + escapeExpressionValue(vectorPrimaryKey) + "\"").build());
        assertMilvusSuccess(result, "Milvus Memory向量删除失败");
    }

    /** Milvus 客户端。 */
    private final ObjectProvider<MilvusServiceClient> milvusServiceClientProvider;

    /** OpenAgentFlow 配置。 */
    private final OpenAgentFlowProperties properties;

    /** 当前发布索引的权威路由，避免上传仍写入旧物理集合。 */
    private final KnowledgeIndexRouteService indexRoutes;

    public MilvusKnowledgeVectorService(ObjectProvider<MilvusServiceClient> milvusServiceClientProvider,
                                        OpenAgentFlowProperties properties) {
        this(milvusServiceClientProvider, properties, null);
    }

    @Autowired
    public MilvusKnowledgeVectorService(ObjectProvider<MilvusServiceClient> milvusServiceClientProvider,
                                        OpenAgentFlowProperties properties, KnowledgeIndexRouteService indexRoutes) {
        this.milvusServiceClientProvider = milvusServiceClientProvider;
        this.properties = properties;
        this.indexRoutes = indexRoutes;
    }

    /**
     * 写入单条知识分片向量，兼容旧调用方。
     *
     * @param collectionName 集合基础名称
     * @param embedding 向量记录
     * @param chunk 分片记录
     * @param vector 向量值
     */
    public void upsertKnowledgeChunk(String collectionName,
                                     KnowledgeEmbeddingEntity embedding,
                                     KnowledgeChunkEntity chunk,
                                     List<Double> vector) {
        upsertKnowledgeChunks(collectionName, List.of(embedding), List.of(chunk), List.of(vector));
    }

    /**
     * 批量写入知识分片向量。
     *
     * @param collectionName 集合基础名称
     * @param embeddings 向量记录列表
     * @param chunks 分片记录列表
     * @param vectors 向量值列表
     */
    public void upsertKnowledgeChunks(String collectionName,
                                      List<KnowledgeEmbeddingEntity> embeddings,
                                      List<KnowledgeChunkEntity> chunks,
                                      List<List<Double>> vectors) {
        upsertKnowledgeChunks(collectionName, embeddings, chunks, vectors, false);
    }

    /** 版本构建显式指定物理目标，不能被当前线上版本重定向。 */
    public void upsertVersionKnowledgeChunks(String collectionName, List<KnowledgeEmbeddingEntity> embeddings,
                                             List<KnowledgeChunkEntity> chunks, List<List<Double>> vectors) {
        upsertKnowledgeChunks(collectionName, embeddings, chunks, vectors, true);
    }

    /** 普通写入跟随 active 版本，版本重建保留独立目标。 */
    private void upsertKnowledgeChunks(String collectionName, List<KnowledgeEmbeddingEntity> embeddings,
                                      List<KnowledgeChunkEntity> chunks, List<List<Double>> vectors,
                                      boolean versionBuild) {
        if (embeddings == null || embeddings.isEmpty()) {
            return;
        }
        if (chunks == null || vectors == null || embeddings.size() != chunks.size() || embeddings.size() != vectors.size()) {
            throw new IllegalArgumentException("Milvus 批量写入数据数量不一致");
        }
        MilvusServiceClient milvusServiceClient = requireMilvusClient();

        int dimension = vectors.get(0).size();
        String baseCollection = StringUtils.hasText(collectionName)
                ? collectionName
                : properties.getMilvus().getDefaultKnowledgeCollection();
        String targetCollection = dimensionCollectionName(baseCollection, dimension);
        if (indexRoutes != null && !embeddings.isEmpty() && !versionBuild) {
            KnowledgeEmbeddingEntity first = embeddings.getFirst();
            KnowledgeIndexRouteService.Route route = indexRoutes.resolveByKnowledgeBaseId(first.getKbId());
            // 显式版本构建目标不重定向；旧入口写入必须跟随当前发布模型。
            if (!route.versionId().isBlank()) {
                if (!java.util.Objects.equals(route.embeddingModelId(), first.getModelId())
                        || route.dimension() != dimension) {
                    throw new IllegalStateException("向量模型或维度与当前发布索引不兼容，请重新生成向量");
                }
                targetCollection = route.collectionName();
            }
        }
        ensureCollection(targetCollection, dimension);

        List<String> primaryKeys = new ArrayList<>(embeddings.size());
        List<String> chunkIds = new ArrayList<>(embeddings.size());
        List<String> documentIds = new ArrayList<>(embeddings.size());
        List<String> kbIds = new ArrayList<>(embeddings.size());
        List<String> contents = new ArrayList<>(embeddings.size());
        List<List<Float>> floatVectors = new ArrayList<>(embeddings.size());

        for (int index = 0; index < embeddings.size(); index++) {
            KnowledgeEmbeddingEntity embedding = embeddings.get(index);
            KnowledgeChunkEntity chunk = chunks.get(index);
            if (!java.util.Objects.equals(embedding.getKbId(), embeddings.getFirst().getKbId())
                    || !java.util.Objects.equals(embedding.getModelId(), embeddings.getFirst().getModelId())
                    || !java.util.Objects.equals(embedding.getKbId(), chunk.getKbId())) {
                throw new IllegalArgumentException("同一向量批次必须属于相同知识库和模型");
            }
            List<Double> vector = vectors.get(index);
            if (vector.size() != dimension) {
                throw new IllegalArgumentException("同一批 Milvus 向量维度必须一致");
            }

            // Milvus Java SDK 要求按字段聚合列数据，这里把行数据转换成列数据。
            primaryKeys.add(embedding.getVectorPrimaryKey());
            chunkIds.add(chunk.getId());
            documentIds.add(chunk.getDocumentId());
            kbIds.add(chunk.getKbId());
            contents.add(truncate(chunk.getContent(), 4096));
            floatVectors.add(vector.stream().map(Double::floatValue).toList());
        }

        UpsertParam upsertParam = UpsertParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(targetCollection)
                .withFields(List.of(
                        new InsertParam.Field("vector_primary_key", primaryKeys),
                        new InsertParam.Field("chunk_id", chunkIds),
                        new InsertParam.Field("document_id", documentIds),
                        new InsertParam.Field("kb_id", kbIds),
                        new InsertParam.Field("content", contents),
                        new InsertParam.Field("embedding", floatVectors)
                ))
                .build();
        R<?> result = milvusServiceClient.upsert(upsertParam);
        assertMilvusSuccess(result, "Milvus 向量批量幂等写入失败");

        // 批量写入后只 flush 一次，避免大文档分片时把网络和磁盘开销放大数百倍。
        milvusServiceClient.flush(FlushParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .addCollectionName(targetCollection)
                .withSyncFlush(false)
                .build());
    }

    /**
     * 原子把稳定别名切换到新物理集合，不存在别名时自动创建。
     *
     * @param physicalCollection 物理集合名称
     * @param alias 稳定查询别名
     */
    public void activateAlias(String physicalCollection, String alias) {
        MilvusServiceClient client = requireMilvusClient();
        R<?> altered = client.alterAlias(AlterAliasParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(physicalCollection)
                .withAlias(alias)
                .build());
        if (altered != null && altered.getStatus() == 0) {
            return;
        }
        R<?> created = client.createAlias(CreateAliasParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(physicalCollection)
                .withAlias(alias)
                .build());
        assertMilvusSuccess(created, "Milvus集合别名激活失败");
    }

    /**
     * 按文档ID删除Milvus实体，删除结果由后台Compaction异步回收空间。
     *
     * @param collectionName 集合基础名称
     * @param dimension 向量维度
     * @param documentId 文档ID
     */
    public void deleteDocument(String collectionName, int dimension, String documentId) {
        String baseCollection = StringUtils.hasText(collectionName)
                ? collectionName : properties.getMilvus().getDefaultKnowledgeCollection();
        String targetCollection = dimensionCollectionName(baseCollection, dimension);
        String safeDocumentId = documentId.replace("\\", "\\\\").replace("\"", "\\\"");
        R<?> result = requireMilvusClient().delete(DeleteParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(targetCollection)
                .withExpr("document_id == \"" + safeDocumentId + "\"")
                .build());
        assertMilvusSuccess(result, "Milvus文档向量删除失败");
    }

    /**
     * 使用 Milvus HNSW 索引执行知识分片近似最近邻检索。
     *
     * @param collectionName 集合基础名称
     * @param kbId 知识库 ID
     * @param queryVector 查询向量
     * @param topK 召回上限
     * @return 按相似度从高到低排列的命中项
     */
    public List<VectorHit> searchKnowledgeChunks(String collectionName,
                                                 String kbId,
                                                 List<Double> queryVector,
                                                 int topK) {
        if (queryVector == null || queryVector.isEmpty() || !StringUtils.hasText(kbId)) {
            return List.of();
        }
        String baseCollection = StringUtils.hasText(collectionName)
                ? collectionName : properties.getMilvus().getDefaultKnowledgeCollection();
        if (baseCollection.matches(".*_d\\d+$") && !baseCollection.endsWith("_d" + queryVector.size())) {
            throw new IllegalArgumentException("查询向量维度与发布索引不匹配");
        }
        String targetCollection = dimensionCollectionName(baseCollection, queryVector.size());
        // 检索为只读操作：错误路由必须明确失败，不能自动创建空集合掩盖问题。

        // Milvus 表达式只接受双引号字符串，因此先转义租户数据中的特殊字符。
        String safeKbId = escapeExpressionValue(kbId);
        SearchParam searchParam = SearchParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(targetCollection)
                .withMetricType(MetricType.COSINE)
                .withVectorFieldName("embedding")
                .withTopK(Math.max(1, topK))
                .withExpr("kb_id == \"" + safeKbId + "\"")
                .withOutFields(List.of("chunk_id", "document_id", "kb_id"))
                .withFloatVectors(List.of(queryVector.stream().map(Double::floatValue).toList()))
                .withParams("{\"ef\":" + Math.max(64, Math.min(512, topK * 4)) + "}")
                .build();
        R<SearchResults> result = requireMilvusClient().search(searchParam);
        assertMilvusSuccess(result, "Milvus 知识向量检索失败");

        SearchResultsWrapper wrapper = new SearchResultsWrapper(result.getData().getResults());
        List<VectorHit> hits = new ArrayList<>();
        for (SearchResultsWrapper.IDScore item : wrapper.getIDScore(0)) {
            Map<String, Object> fields = item.getFieldValues();
            hits.add(new VectorHit(
                    String.valueOf(fields.getOrDefault("chunk_id", "")),
                    String.valueOf(fields.getOrDefault("document_id", "")),
                    String.valueOf(fields.getOrDefault("kb_id", kbId)),
                    Math.max(0D, Math.min(1D, item.getScore()))));
        }
        return hits;
    }

    /**
     * 按维度生成实际集合名，避免旧 256 维兜底向量和真实 2048 维模型向量冲突。
     *
     * @param baseCollection 集合基础名称
     * @param dimension 向量维度
     * @return 实际集合名称
     */
    private String dimensionCollectionName(String baseCollection, int dimension) {
        String suffix = "_d" + dimension;
        return baseCollection.endsWith(suffix) ? baseCollection : baseCollection + suffix;
    }

    /**
     * 转义 Milvus 标量过滤表达式中的字符串值。
     *
     * @param value 原始值
     * @return 可安全嵌入表达式的值
     */
    private String escapeExpressionValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * 确保 Milvus 集合存在并已加载。
     *
     * @param collectionName 集合名称
     * @param dimension 向量维度
     */
    private void ensureCollection(String collectionName, int dimension) {
        MilvusServiceClient milvusServiceClient = requireMilvusClient();
        R<Boolean> hasCollection = milvusServiceClient.hasCollection(HasCollectionParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(collectionName)
                .build());
        assertMilvusSuccess(hasCollection, "Milvus 集合检查失败");
        if (Boolean.TRUE.equals(hasCollection.getData())) {
            loadCollection(collectionName);
            return;
        }

        List<FieldType> fields = new ArrayList<>();
        fields.add(FieldType.newBuilder()
                .withName("vector_primary_key")
                .withDataType(DataType.VarChar)
                .withPrimaryKey(true)
                .withAutoID(false)
                .withMaxLength(160)
                .build());
        fields.add(varCharField("chunk_id", 80));
        fields.add(varCharField("document_id", 80));
        fields.add(varCharField("kb_id", 80));
        fields.add(varCharField("content", 4096));
        fields.add(FieldType.newBuilder()
                .withName("embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(dimension)
                .build());

        R<?> createResult = milvusServiceClient.createCollection(CreateCollectionParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(collectionName)
                .withDescription("OpenAgentFlow 知识库分片向量")
                .withShardsNum(2)
                .withFieldTypes(fields)
                .build());
        assertMilvusSuccess(createResult, "Milvus 集合创建失败");

        R<?> indexResult = milvusServiceClient.createIndex(CreateIndexParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(collectionName)
                .withFieldName("embedding")
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.COSINE)
                .withExtraParam("{\"M\":16,\"efConstruction\":128}")
                .withSyncMode(Boolean.FALSE)
                .build());
        assertMilvusSuccess(indexResult, "Milvus 索引创建失败");
        loadCollection(collectionName);
    }

    /** 确保Memory专用集合存在并已加载。 */
    private void ensureMemoryCollection(String collectionName, int dimension) {
        MilvusServiceClient client = requireMilvusClient();
        R<Boolean> exists = client.hasCollection(HasCollectionParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName()).withCollectionName(collectionName).build());
        assertMilvusSuccess(exists, "Milvus Memory集合检查失败");
        if (Boolean.TRUE.equals(exists.getData())) { loadCollection(collectionName); return; }
        List<FieldType> fields = new ArrayList<>();
        fields.add(FieldType.newBuilder().withName("vector_primary_key").withDataType(DataType.VarChar)
                .withPrimaryKey(true).withAutoID(false).withMaxLength(160).build());
        fields.add(varCharField("memory_id", 80));
        fields.add(varCharField("workspace_id", 80));
        fields.add(varCharField("agent_id", 80));
        fields.add(varCharField("user_id", 80));
        fields.add(varCharField("privacy_scope", 32));
        fields.add(varCharField("content", 4096));
        fields.add(FieldType.newBuilder().withName("embedding").withDataType(DataType.FloatVector).withDimension(dimension).build());
        assertMilvusSuccess(client.createCollection(CreateCollectionParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName()).withCollectionName(collectionName)
                .withDescription("OpenAgentFlow Agent Memory向量").withShardsNum(2).withFieldTypes(fields).build()),
                "Milvus Memory集合创建失败");
        assertMilvusSuccess(client.createIndex(CreateIndexParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName()).withCollectionName(collectionName)
                .withFieldName("embedding").withIndexType(IndexType.HNSW).withMetricType(MetricType.COSINE)
                .withExtraParam("{\"M\":16,\"efConstruction\":128}").withSyncMode(false).build()),
                "Milvus Memory索引创建失败");
        loadCollection(collectionName);
    }

    private String value(String value) { return value == null ? "" : value; }

    /**
     * 加载集合到 Milvus 查询节点。
     *
     * @param collectionName 集合名称
     */
    private void loadCollection(String collectionName) {
        MilvusServiceClient milvusServiceClient = requireMilvusClient();
        R<?> loadResult = milvusServiceClient.loadCollection(LoadCollectionParam.newBuilder()
                .withDatabaseName(properties.getMilvus().getDatabaseName())
                .withCollectionName(collectionName)
                .withSyncLoad(false)
                .build());
        assertMilvusSuccess(loadResult, "Milvus 集合加载失败");
    }

    /**
     * 创建 VarChar 字段定义。
     *
     * @param name 字段名称
     * @param maxLength 最大长度
     * @return 字段定义
     */
    private FieldType varCharField(String name, int maxLength) {
        return FieldType.newBuilder()
                .withName(name)
                .withDataType(DataType.VarChar)
                .withMaxLength(maxLength)
                .build();
    }

    /**
     * 校验 Milvus 调用结果。
     *
     * @param result Milvus SDK 返回对象
     * @param message 业务错误消息
     */
    private void assertMilvusSuccess(R<?> result, String message) {
        if (result == null || result.getStatus() == null || result.getStatus() != 0) {
            String detail = result == null ? "无返回" : result.getMessage();
            throw new IllegalStateException(message + "：" + detail);
        }
    }

    /**
     * 获取 Milvus 客户端；开发环境未启动 Milvus 时抛出业务可捕获异常，由上层保留 MySQL 向量兜底。
     *
     * @return Milvus 客户端
     */
    private MilvusServiceClient requireMilvusClient() {
        MilvusServiceClient client = milvusServiceClientProvider.getIfAvailable();
        if (client == null) {
            throw new IllegalStateException("Milvus 未启用或未连接，已使用 MySQL 向量兜底");
        }
        return client;
    }

    /**
     * 截断写入 Milvus 的分片预览文本。
     *
     * @param text 原始文本
     * @param maxLength 最大长度
     * @return 截断后的文本
     */
    private String truncate(String text, int maxLength) {
        if (text == null || text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength);
    }
}
