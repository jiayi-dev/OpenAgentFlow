package com.openagentflow.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.entity.AsyncTaskEntity;
import com.openagentflow.entity.KnowledgeBaseEntity;
import com.openagentflow.entity.KnowledgeChunkEntity;
import com.openagentflow.entity.KnowledgeDocumentEntity;
import com.openagentflow.entity.KnowledgeEmbeddingEntity;
import com.openagentflow.entity.ModelConfigEntity;
import com.openagentflow.mapper.KnowledgeBaseMapper;
import com.openagentflow.mapper.KnowledgeDocumentMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.DigestUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * 文档物理DAG阶段处理器。
 *
 * <p>每个阶段都是独立Kafka任务。解析和切片生成对象存储产物，Embedding与向量写入按分片扇出，
 * 最后一个分片通过幂等键创建下一阶段，实现可水平扩展的Fan-out/Fan-in。</p>
 */
@Service
public class PhysicalDocumentPipelineService implements DistributedTaskHandler {

    /** 默认向量集合ID。 */
    private static final String DEFAULT_VECTOR_COLLECTION_ID = "70000000-0000-0000-0000-000000000101";

    /** 异步任务服务。 */
    private final AsyncTaskService asyncTaskService;

    /** 阶段状态服务。 */
    private final AsyncTaskStageService stageService;

    /** 文档解析服务。 */
    private final DocumentParseService documentParseService;

    /** 文档切片服务。 */
    private final KnowledgeChunkingService chunkingService;

    /** Embedding服务。 */
    private final EmbeddingService embeddingService;

    /** Milvus向量写入服务。 */
    private final MilvusKnowledgeVectorService milvusService;

    /** 对象存储服务。 */
    private final SharedObjectStorageService objectStorageService;

    /** 知识库Mapper。 */
    private final KnowledgeBaseMapper knowledgeBaseMapper;

    /** 文档Mapper。 */
    private final KnowledgeDocumentMapper knowledgeDocumentMapper;

    /** JDBC工具。 */
    private final JdbcTemplate jdbcTemplate;

    /** JSON工具。 */
    private final ObjectMapper objectMapper;

    /** OpenSearch关键词索引服务。 */
    private final KeywordSearchService keywordSearchService;

    /** 单个Embedding分片最大切片数。 */
    private final int embeddingShardSize;

    /** 阶段节点与Outbox批量原子提交模板。 */
    private final TransactionTemplate transactionTemplate;

    /** 发布模型与写入目标的路由。 */
    private final KnowledgeIndexRouteService indexRoutes;

    public PhysicalDocumentPipelineService(AsyncTaskService asyncTaskService,
                                           AsyncTaskStageService stageService,
                                           DocumentParseService documentParseService,
                                           KnowledgeChunkingService chunkingService,
                                           EmbeddingService embeddingService,
                                           MilvusKnowledgeVectorService milvusService,
                                           SharedObjectStorageService objectStorageService,
                                           KnowledgeBaseMapper knowledgeBaseMapper,
                                           KnowledgeDocumentMapper knowledgeDocumentMapper,
                                           JdbcTemplate jdbcTemplate,
                                           ObjectMapper objectMapper,
                                           KeywordSearchService keywordSearchService,
                                           @Value("${openagentflow.document-pipeline.embedding-shard-size:16}") int embeddingShardSize,
                                           TransactionTemplate transactionTemplate,
                                           KnowledgeIndexRouteService indexRoutes) {
        this.asyncTaskService = asyncTaskService;
        this.stageService = stageService;
        this.documentParseService = documentParseService;
        this.chunkingService = chunkingService;
        this.embeddingService = embeddingService;
        this.milvusService = milvusService;
        this.objectStorageService = objectStorageService;
        this.knowledgeBaseMapper = knowledgeBaseMapper;
        this.knowledgeDocumentMapper = knowledgeDocumentMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.keywordSearchService = keywordSearchService;
        this.embeddingShardSize = Math.max(16, Math.min(embeddingShardSize, 1000));
        this.transactionTemplate = transactionTemplate;
        this.indexRoutes = indexRoutes;
    }

    /** 返回主阶段类型。 */
    @Override
    public String taskType() {
        return "DOCUMENT_PIPELINE_PARSE";
    }

    /** 返回全部物理阶段任务类型。 */
    @Override
    public Set<String> taskTypes() {
        return Set.of("DOCUMENT_PIPELINE_PARSE", "DOCUMENT_PIPELINE_CHUNK", "DOCUMENT_PIPELINE_EMBED",
                "DOCUMENT_PIPELINE_VECTOR_WRITE", "DOCUMENT_PIPELINE_FINALIZE");
    }

    /** 根据任务类型执行物理阶段。 */
    @Override
    public Map<String, Object> executeDistributedTask(AsyncTaskEntity task) {
        return switch (task.getTaskType()) {
            case "DOCUMENT_PIPELINE_PARSE" -> runStage(task, "parse", "解析文档", 1, () -> parse(task));
            case "DOCUMENT_PIPELINE_CHUNK" -> runStage(task, "chunk", "流式切片", 2, () -> chunk(task));
            case "DOCUMENT_PIPELINE_EMBED" -> runStage(task, "embedding", "生成向量分片", 3, () -> embed(task));
            case "DOCUMENT_PIPELINE_VECTOR_WRITE" -> runStage(task, "persist", "写入向量分片", 4, () -> writeVector(task));
            case "DOCUMENT_PIPELINE_FINALIZE" -> runStage(task, "index", "完成知识索引", 5, () -> finalizeDocument(task));
            default -> throw new IllegalArgumentException("不支持的文档DAG任务类型：" + task.getTaskType());
        };
    }

    /** 包装阶段生命周期，失败时由阶段服务写入明确错误。 */
    private Map<String, Object> runStage(AsyncTaskEntity task,
                                         String stageCode,
                                         String stageName,
                                         int order,
                                         Supplier<Map<String, Object>> action) {
        Map<String, Object> input = payload(task);
        stageService.start(task.getId(), stageCode, stageName, order, input);
        try {
            Map<String, Object> result = action.get();
            stageService.succeed(task.getId(), stageCode, result);
            return result;
        } catch (Exception exception) {
            stageService.fail(task.getId(), stageCode, exception.getMessage());
            throw exception;
        }
    }

    /** 解析原文件并投递切片阶段。 */
    private Map<String, Object> parse(AsyncTaskEntity task) {
        Map<String, Object> payload = payload(task);
        KnowledgeDocumentEntity document = requireDocument(text(payload.get("documentId")));
        assertCurrentPipeline(document, payload, rootTaskId(task, payload));
        Path localFile = objectStorageService.materializeTempFile(document.getStorageBucket(), document.getStorageKey(), document.getFileExt());
        try {
            String parsedText = documentParseService.parse(localFile, document.getFileExt());
            String currentRootTaskId = rootTaskId(task, payload);
            jdbcTemplate.update("""
                    UPDATE knowledge_document SET metadata=JSON_SET(COALESCE(metadata,JSON_OBJECT()),
                      '$.expectedChunkCount',0,'$.expectedEmbeddingCount',0)
                    WHERE id=? AND current_pipeline_root_id=? AND pipeline_generation=?
                    """, document.getId(), currentRootTaskId, integer(payload.get("pipelineGeneration"), 0));
            String artifactKey = artifactKey(document, currentRootTaskId, "parsed/parsed.txt");
            objectStorageService.put(artifactKey, parsedText.getBytes(StandardCharsets.UTF_8), "text/plain;charset=UTF-8");
            Map<String, Object> next = withBase(payload, task);
            next.put("parsedArtifactKey", artifactKey);
            createNodeAndTask(task, "chunk", "DOCUMENT_PIPELINE_CHUNK", 0, 1,
                    "切分知识文档：" + document.getDocName(), next);
            updateDocumentProgress(document.getId(), payload, currentRootTaskId,
                    "parsed", 20, "文档解析完成，已投递物理切片任务");
            return Map.of("artifactKey", artifactKey, "textLength", parsedText.length());
        } finally {
            try { Files.deleteIfExists(localFile); } catch (Exception ignored) { }
        }
    }

    /** 生成稳定ID的切片清单并扇出Embedding任务。 */
    private Map<String, Object> chunk(AsyncTaskEntity task) {
        Map<String, Object> payload = payload(task);
        KnowledgeDocumentEntity document = requireDocument(text(payload.get("documentId")));
        assertCurrentPipeline(document, payload, rootTaskId(task, payload));
        KnowledgeBaseEntity kb = requireKnowledgeBase(document.getKbId());
        String parsedText = readUtf8(document.getStorageBucket(), text(payload.get("parsedArtifactKey")));
        List<KnowledgeChunkingService.ChunkSegment> sourceSegments = chunkingService.splitSegments(
                parsedText, kb.getChunkStrategy(), kb.getChunkSize(), kb.getChunkOverlap());
        List<PipelineSegment> segments = stableSegments(sourceSegments, kb, document);
        List<PipelineSegment> embeddingSegments = segments.stream().filter(PipelineSegment::embeddingEnabled).toList();
        if (embeddingSegments.isEmpty()) {
            throw new IllegalStateException("文档没有生成可向量化分片");
        }
        Map<String, PipelineSegment> allSegments = new LinkedHashMap<>();
        segments.forEach(segment -> allSegments.put(segment.id(), segment));
        int shardTotal = Math.max(1, (embeddingSegments.size() + embeddingShardSize - 1) / embeddingShardSize);
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            for (int shardNo = 0; shardNo < shardTotal; shardNo++) {
                int start = shardNo * embeddingShardSize;
                int end = Math.min(start + embeddingShardSize, embeddingSegments.size());
                List<PipelineSegment> shardSegments = new ArrayList<>();
                Set<String> includedIds = new java.util.LinkedHashSet<>();
                for (PipelineSegment segment : embeddingSegments.subList(start, end)) {
                    // 子分片写入前携带其父分片，确保任意Worker只读取当前分片工件即可独立入库。
                    if (segment.parentChunkId() != null && includedIds.add(segment.parentChunkId())) {
                        PipelineSegment parent = allSegments.get(segment.parentChunkId());
                        if (parent != null) shardSegments.add(parent);
                    }
                    if (includedIds.add(segment.id())) shardSegments.add(segment);
                }
                String shardArtifactKey = artifactKey(document, rootTaskId(task, payload), "chunks/shard-" + shardNo + ".json");
                putJson(shardArtifactKey, shardSegments);
                Map<String, Object> next = withBase(payload, task);
                next.put("segmentArtifactKey", shardArtifactKey);
                next.put("expectedItemCount", end - start);
                createNodeAndTask(task, "embedding", "DOCUMENT_PIPELINE_EMBED", shardNo, shardTotal,
                        "生成文档向量分片 " + (shardNo + 1) + "/" + shardTotal, next);
            }
            jdbcTemplate.update("""
                    UPDATE knowledge_document SET metadata=JSON_SET(COALESCE(metadata,JSON_OBJECT()),
                      '$.expectedChunkCount',?,'$.expectedEmbeddingCount',?,'$.expectedShardTotal',?)
                    WHERE id=? AND current_pipeline_root_id=? AND pipeline_generation=?
                    """, segments.size(), embeddingSegments.size(), shardTotal, document.getId(),
                    rootTaskId(task, payload), integer(payload.get("pipelineGeneration"), 0));
        });
        updateDocumentProgress(document.getId(), payload, rootTaskId(task, payload),
                "chunked", 35, "已生成" + segments.size() + "个分片并扇出" + shardTotal + "个Embedding任务");
        return Map.of("artifactPrefix", artifactKey(document, rootTaskId(task, payload), "chunks/"), "chunkCount", segments.size(),
                "embeddingChunkCount", embeddingSegments.size(), "shardTotal", shardTotal);
    }

    /** 调用真实Embedding模型并保存当前向量分片产物。 */
    private Map<String, Object> embed(AsyncTaskEntity task) {
        Map<String, Object> payload = payload(task);
        KnowledgeDocumentEntity document = requireDocument(text(payload.get("documentId")));
        assertCurrentPipeline(document, payload, rootTaskId(task, payload));
        KnowledgeBaseEntity kb = requireKnowledgeBase(document.getKbId());
        List<PipelineSegment> segments = readJson(document.getStorageBucket(), text(payload.get("segmentArtifactKey")), new TypeReference<>() { });
        List<PipelineSegment> embeddingSegments = segments.stream().filter(PipelineSegment::embeddingEnabled).toList();
        // 分片工件只包含当前批次及其父分片，避免每个Kafka Worker重复加载整份文档清单。
        List<PipelineSegment> shard = embeddingSegments;
        ModelConfigEntity model = embeddingService.resolveEmbeddingModel(indexRoutes.resolve(kb).embeddingModelId());
        EmbeddingBatchResult result = embeddingService.embedWithTrace(model, shard.stream().map(PipelineSegment::content).toList());
        DocumentPipelineReliability.requireVectorCardinality(shard.size(), result.getVectors(), "Embedding分片");
        List<VectorItem> vectors = new ArrayList<>();
        for (int index = 0; index < shard.size(); index++) {
            vectors.add(new VectorItem(shard.get(index).id(), result.getVectors().get(index)));
        }
        String artifactKey = artifactKey(document, rootTaskId(task, payload), "vectors/shard-" + value(task.getShardNo()) + ".json");
        putJson(artifactKey, vectors);
        Map<String, Object> output = Map.of("artifactKey", artifactKey, "vectorCount", vectors.size(),
                "dimension", result.getDimension(), "modelId", model.getId(), "segmentArtifactKey", text(payload.get("segmentArtifactKey")));
        updateNodeOutput(task.getId(), output);
        if (allNodesSuccessful(rootTaskId(task, payload), "embedding")) {
            scheduleVectorWriteTasks(task, document, payload);
        }
        updateDocumentProgress(document.getId(), payload, rootTaskId(task, payload),
                "embedding", 35 + progress(task, 30), "Embedding分片" + (value(task.getShardNo()) + 1) + "已完成");
        return output;
    }

    /** 幂等持久化当前分片并写入Milvus。 */
    private Map<String, Object> writeVector(AsyncTaskEntity task) {
        Map<String, Object> payload = payload(task);
        KnowledgeDocumentEntity document = requireDocument(text(payload.get("documentId")));
        assertCurrentPipeline(document, payload, rootTaskId(task, payload));
        KnowledgeBaseEntity kb = requireKnowledgeBase(document.getKbId());
        List<PipelineSegment> segments = readJson(document.getStorageBucket(), text(payload.get("segmentArtifactKey")), new TypeReference<>() { });
        List<VectorItem> vectors = readJson(document.getStorageBucket(), text(payload.get("vectorArtifactKey")), new TypeReference<>() { });
        Map<String, PipelineSegment> segmentMap = new LinkedHashMap<>();
        segments.forEach(segment -> segmentMap.put(segment.id(), segment));
        // 使用向量产物实际采用的模型，不能在模型配置变更后重新贴标签。
        String artifactModelId = text(payload.get("embeddingModelId"));
        ModelConfigEntity model = embeddingService.resolveEmbeddingModel(artifactModelId.isBlank()
                ? indexRoutes.resolve(kb).embeddingModelId() : artifactModelId);
        List<KnowledgeEmbeddingEntity> embeddingEntities = new ArrayList<>();
        List<KnowledgeChunkEntity> chunkEntities = new ArrayList<>();
        List<List<Double>> vectorValues = new ArrayList<>();
        for (VectorItem vector : vectors) {
            PipelineSegment segment = segmentMap.get(vector.chunkId());
            if (segment == null) continue;
            if (segment.parentChunkId() != null) {
                insertChunkIgnore(segmentMap.get(segment.parentChunkId()), kb, document);
            }
            insertChunkIgnore(segment, kb, document);
            KnowledgeChunkEntity chunk = toChunkEntity(segment, kb, document);
            KnowledgeEmbeddingEntity embedding = insertEmbedding(task, chunk, kb, model, vector.vector());
            chunkEntities.add(chunk);
            embeddingEntities.add(embedding);
            vectorValues.add(vector.vector());
        }
        DocumentPipelineReliability.requireVectorCardinality(vectors.size(), embeddingEntities, "向量持久化分片");
        milvusService.upsertKnowledgeChunks(kb.getMilvusCollectionName(), embeddingEntities, chunkEntities, vectorValues);
        keywordSearchService.indexChunks(kb.getId(), chunkEntities);
        LocalDateTime now = LocalDateTime.now();
        for (KnowledgeEmbeddingEntity embedding : embeddingEntities) {
            jdbcTemplate.update("UPDATE knowledge_embedding SET sync_status='synced', last_synced_at=? WHERE id=?", now, embedding.getId());
        }
        Map<String, Object> output = Map.of("writtenCount", vectors.size(), "milvusSynced", true);
        updateNodeOutput(task.getId(), output);
        if (allNodesSuccessful(rootTaskId(task, payload), "persist")) {
            scheduleFinalizeTask(task, document, payload);
        }
        updateDocumentProgress(document.getId(), payload, rootTaskId(task, payload),
                "persist", 65 + progress(task, 30), "向量写入分片" + (value(task.getShardNo()) + 1) + "已完成");
        return output;
    }

    /** 收口文档状态并完成根任务。 */
    private Map<String, Object> finalizeDocument(AsyncTaskEntity task) {
        Map<String, Object> payload = payload(task);
        String documentId = text(payload.get("documentId"));
        KnowledgeDocumentEntity document = requireDocument(documentId);
        String rootTaskId = rootTaskId(task, payload);
        assertCurrentPipeline(document, payload, rootTaskId);
        long chunkCount = scalar("SELECT COUNT(1) FROM knowledge_chunk WHERE document_id=?", documentId);
        long embeddingCount = scalar("SELECT COUNT(1) FROM knowledge_embedding e JOIN knowledge_chunk c ON c.id=e.chunk_id WHERE c.document_id=? AND e.sync_status='synced'", documentId);
        long expectedChunkCount = scalar("SELECT COALESCE(JSON_EXTRACT(metadata,'$.expectedChunkCount'),0) FROM knowledge_document WHERE id=?", documentId);
        long expectedEmbeddingCount = scalar("SELECT COALESCE(JSON_EXTRACT(metadata,'$.expectedEmbeddingCount'),0) FROM knowledge_document WHERE id=?", documentId);
        DocumentPipelineReliability.requireFinalCardinality(expectedChunkCount, chunkCount,
                expectedEmbeddingCount, embeddingCount);
        jdbcTemplate.update("""
                UPDATE knowledge_document
                SET parse_status='parsed', parse_error=NULL,
                    metadata=JSON_SET(COALESCE(metadata,JSON_OBJECT()),'$.processStage','done','$.processStageLabel','处理完成',
                      '$.progressPercent',100,'$.lastMessage','物理DAG全部分片处理完成','$.milvusSynced',true)
                WHERE id=? AND current_pipeline_root_id=? AND pipeline_generation=?
                """, documentId, rootTaskId, integer(payload.get("pipelineGeneration"), 0));
        return Map.of("documentId", documentId, "chunkCount", chunkCount, "embeddingCount", embeddingCount, "milvusSynced", true);
    }

    /** 最后一个Embedding分片完成后，按分片产物创建向量写入任务。 */
    private void scheduleVectorWriteTasks(AsyncTaskEntity task, KnowledgeDocumentEntity document, Map<String, Object> payload) {
        transactionTemplate.executeWithoutResult(status -> {
            // 锁定文档行并二次判断，保证并发Fan-in只有一个Worker负责发布下一阶段。
            jdbcTemplate.queryForObject("SELECT id FROM knowledge_document WHERE id=? FOR UPDATE",
                    String.class, document.getId());
            assertCurrentPipeline(document, payload, rootTaskId(task, payload));
            long existing = scalar("SELECT COUNT(1) FROM document_pipeline_node WHERE root_task_id=? AND stage_code='persist'",
                    rootTaskId(task, payload));
            if (existing > 0) return;
            List<Map<String, Object>> nodes = jdbcTemplate.queryForList("""
                    SELECT shard_no, shard_total,output_json FROM document_pipeline_node
                    WHERE root_task_id=? AND stage_code='embedding' AND status='success' ORDER BY shard_no
                    """, rootTaskId(task, payload));
            // 同一阶段的节点、任务和Outbox必须整体提交，避免消费者只看到部分分片。
            for (Map<String, Object> node : nodes) {
                Map<String, Object> embeddingOutput = map(text(node.get("output_json")));
                int shardNo = integer(node.get("shard_no"), 0);
                int shardTotal = integer(node.get("shard_total"), nodes.size());
                Map<String, Object> next = withBase(payload, task);
                next.put("segmentArtifactKey", embeddingOutput.get("segmentArtifactKey"));
                next.put("vectorArtifactKey", embeddingOutput.get("artifactKey"));
                next.put("embeddingModelId", embeddingOutput.get("modelId"));
                next.put("expectedItemCount", integer(embeddingOutput.get("vectorCount"), 0));
                createNodeAndTask(task, "persist", "DOCUMENT_PIPELINE_VECTOR_WRITE", shardNo, shardTotal,
                        "写入文档向量分片 " + (shardNo + 1) + "/" + shardTotal, next);
            }
        });
    }

    /** 最后一个持久化Worker通过行锁幂等发布唯一收口任务。 */
    private void scheduleFinalizeTask(AsyncTaskEntity task, KnowledgeDocumentEntity document, Map<String, Object> payload) {
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.queryForObject("SELECT id FROM knowledge_document WHERE id=? FOR UPDATE",
                    String.class, document.getId());
            assertCurrentPipeline(document, payload, rootTaskId(task, payload));
            long existing = scalar("SELECT COUNT(1) FROM document_pipeline_node WHERE root_task_id=? AND stage_code='index'",
                    rootTaskId(task, payload));
            if (existing > 0) return;
            Map<String, Object> next = withBase(payload, task);
            next.put("expectedItemCount", 1);
            createNodeAndTask(task, "index", "DOCUMENT_PIPELINE_FINALIZE", 0, 1,
                    "完成知识文档索引：" + document.getDocName(), next);
        });
    }

    /** 创建DAG节点和对应Kafka子任务。 */
    private void createNodeAndTask(AsyncTaskEntity parent,
                                   String stageCode,
                                   String taskType,
                                   int shardNo,
                                   int shardTotal,
                                   String taskName,
                                   Map<String, Object> payload) {
        String rootTaskId = rootTaskId(parent, payload);
        String nodeKey = rootTaskId + ":" + stageCode + ":" + shardNo;
        jdbcTemplate.update("""
                INSERT IGNORE INTO document_pipeline_node
                  (id,root_task_id,generation_no,document_id,kb_id,stage_code,shard_no,shard_total,dependency_count,status,
                   idempotency_key,input_json,expected_item_count,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,0,'queued',?,?,?,NOW(3),NOW(3))
                """, UUID.randomUUID().toString(), rootTaskId, integer(payload.get("pipelineGeneration"), 0),
                payload.get("documentId"), payload.get("kbId"), stageCode, shardNo, shardTotal, nodeKey, json(payload),
                integer(payload.get("expectedItemCount"), 0));
        AsyncTaskEntity child = asyncTaskService.createDagChildTask(parent, taskName, taskType, shardNo, shardTotal,
                nodeKey + ":task", payload);
        jdbcTemplate.update("UPDATE document_pipeline_node SET task_id=? WHERE idempotency_key=?", child.getId(), nodeKey);
    }

    /** 构造稳定分片ID和父子引用。 */
    private List<PipelineSegment> stableSegments(List<KnowledgeChunkingService.ChunkSegment> source,
                                                 KnowledgeBaseEntity kb,
                                                 KnowledgeDocumentEntity document) {
        Map<Integer, String> parentIds = new LinkedHashMap<>();
        List<PipelineSegment> result = new ArrayList<>();
        int chunkNo = 1;
        for (KnowledgeChunkingService.ChunkSegment segment : source) {
            int assignedChunkNo = chunkNo++;
            String contentHash = DigestUtils.md5DigestAsHex(segment.content().getBytes(StandardCharsets.UTF_8));
            // 子分片序号和偏移量可能在不同父分片内重新计数，必须加入全局分片号和内容哈希避免稳定ID碰撞。
            String id = DocumentPipelineReliability.stableChunkId(document.getId(), assignedChunkNo, segment.level(),
                    segment.ordinal(), segment.parentOrdinal(), segment.startOffset(), segment.content());
            String parentId = segment.parentOrdinal() == null ? null : parentIds.get(segment.parentOrdinal());
            if ("parent".equals(segment.level())) parentIds.put(segment.ordinal(), id);
            result.add(new PipelineSegment(id, parentId, assignedChunkNo, segment.level(), segment.content(),
                    segment.sectionTitle(), segment.sectionPath(), segment.paragraphNo(), segment.startOffset(), segment.endOffset(),
                    chunkingService.estimateTokens(segment.content()),
                    contentHash, segment.embeddingEnabled()));
        }
        return result;
    }

    /** 幂等插入知识分片。 */
    private void insertChunkIgnore(PipelineSegment segment, KnowledgeBaseEntity kb, KnowledgeDocumentEntity document) {
        if (segment == null) return;
        jdbcTemplate.update("""
                INSERT IGNORE INTO knowledge_chunk
                  (id,kb_id,document_id,chunk_no,parent_chunk_id,chunk_level,title,section_title,section_path,paragraph_no,
                   content,token_count,start_offset,end_offset,strategy_version,content_hash,source_hash,metadata,status,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'dag-v1',?,?,JSON_OBJECT('pipeline','physical-dag'),'active',NOW(3))
                """, segment.id(), kb.getId(), document.getId(), segment.chunkNo(), segment.parentChunkId(), segment.level(),
                segment.sectionTitle(), segment.sectionTitle(), segment.sectionPath(), segment.paragraphNo(), segment.content(),
                segment.tokenCount(), segment.startOffset(), segment.endOffset(), segment.contentHash(), document.getFileHash());
    }

    /** 幂等插入向量映射。 */
    private KnowledgeEmbeddingEntity insertEmbedding(AsyncTaskEntity task,
                                                      KnowledgeChunkEntity chunk,
                                                      KnowledgeBaseEntity kb,
                                                      ModelConfigEntity model,
                                                      List<Double> vector) {
        String id = UUID.nameUUIDFromBytes((chunk.getId() + ":" + model.getId()).getBytes(StandardCharsets.UTF_8)).toString();
        String collectionId = kb.getVectorCollectionId() == null ? DEFAULT_VECTOR_COLLECTION_ID : kb.getVectorCollectionId();
        jdbcTemplate.update("""
                INSERT INTO knowledge_embedding
                  (id,chunk_id,kb_id,model_id,vector_collection_id,milvus_collection_name,milvus_partition_name,
                   vector_primary_key,sync_status,embedding_json,embedding_dim,content_hash,created_at)
                VALUES (?,?,?,?,?,?,?,?, 'pending',?,?,?,NOW(3))
                ON DUPLICATE KEY UPDATE sync_status=IF(sync_status='synced','synced','pending'), embedding_json=VALUES(embedding_json),embedding_dim=VALUES(embedding_dim)
                """, id, chunk.getId(), kb.getId(), model.getId(), collectionId, kb.getMilvusCollectionName(),
                kb.getMilvusPartitionName(), chunk.getId(), json(vector), vector.size(), chunk.getContentHash());
        KnowledgeEmbeddingEntity entity = new KnowledgeEmbeddingEntity();
        entity.setId(id);
        entity.setChunkId(chunk.getId());
        entity.setKbId(kb.getId());
        entity.setModelId(model.getId());
        entity.setVectorCollectionId(collectionId);
        entity.setMilvusCollectionName(kb.getMilvusCollectionName());
        entity.setMilvusPartitionName(kb.getMilvusPartitionName());
        entity.setVectorPrimaryKey(chunk.getId());
        entity.setSyncStatus("pending");
        entity.setEmbeddingDim(vector.size());
        entity.setContentHash(chunk.getContentHash());
        return entity;
    }

    /** 转换为Milvus写入需要的分片实体。 */
    private KnowledgeChunkEntity toChunkEntity(PipelineSegment segment, KnowledgeBaseEntity kb, KnowledgeDocumentEntity document) {
        KnowledgeChunkEntity entity = new KnowledgeChunkEntity();
        entity.setId(segment.id());
        entity.setKbId(kb.getId());
        entity.setDocumentId(document.getId());
        entity.setChunkNo(segment.chunkNo());
        entity.setParentChunkId(segment.parentChunkId());
        entity.setChunkLevel(segment.level());
        entity.setContent(segment.content());
        entity.setContentHash(segment.contentHash());
        return entity;
    }

    /** 判断同一阶段所有物理节点是否成功。 */
    private boolean allNodesSuccessful(String rootTaskId, String stageCode) {
        Long total = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM document_pipeline_node WHERE root_task_id=? AND stage_code=?", Long.class, rootTaskId, stageCode);
        Long success = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM document_pipeline_node WHERE root_task_id=? AND stage_code=? AND status='success'", Long.class, rootTaskId, stageCode);
        Integer expected = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(shard_total),0) FROM document_pipeline_node WHERE root_task_id=? AND stage_code=?", Integer.class, rootTaskId, stageCode);
        // Fan-in必须等待预期分片节点全部创建且全部成功，不能把“当前已创建节点全部成功”误认为阶段完成。
        return DocumentPipelineReliability.stageComplete(total == null ? 0 : total, success == null ? 0 : success,
                expected == null ? 0 : expected);
    }

    /** 把当前节点产物写入DAG表，供Fan-in阶段读取。 */
    private void updateNodeOutput(String taskId, Map<String, Object> output) {
        int actualItemCount = integer(output.get("vectorCount"), integer(output.get("writtenCount"), 0));
        jdbcTemplate.update("""
                UPDATE document_pipeline_node
                SET status='success',output_json=?,actual_item_count=?,finished_at=NOW(3),updated_at=NOW(3)
                WHERE task_id=?
                """, json(output), actualItemCount, taskId);
    }

    /** 更新文档面向前端的总体进度。 */
    private void updateDocumentProgress(String documentId,
                                        Map<String, Object> payload,
                                        String rootTaskId,
                                        String stage,
                                        int progress,
                                        String message) {
        jdbcTemplate.update("""
                UPDATE knowledge_document SET metadata=JSON_SET(COALESCE(metadata,JSON_OBJECT()),
                  '$.processStage',?,'$.progressPercent',?,'$.lastMessage',?)
                WHERE id=? AND current_pipeline_root_id=? AND pipeline_generation=?
                """, stage, Math.min(99, progress), message, documentId, rootTaskId,
                integer(payload.get("pipelineGeneration"), 0));
    }

    private int progress(AsyncTaskEntity task, int span) {
        return (int) Math.round(((value(task.getShardNo()) + 1D) / Math.max(1, value(task.getShardTotal()))) * span);
    }

    private Map<String, Object> withBase(Map<String, Object> payload, AsyncTaskEntity task) {
        Map<String, Object> result = new LinkedHashMap<>(payload);
        result.put("rootTaskId", rootTaskId(task, payload));
        return result;
    }

    private String rootTaskId(AsyncTaskEntity task, Map<String, Object> payload) {
        String fromPayload = text(payload.get("rootTaskId"));
        if (!fromPayload.isBlank()) return fromPayload;
        return task.getRootTaskId() == null ? task.getId() : task.getRootTaskId();
    }

    private String artifactKey(KnowledgeDocumentEntity document, String rootTaskId, String suffix) {
        return "artifacts/knowledge/" + document.getKbId() + "/" + document.getId() + "/" + rootTaskId + "/" + suffix;
    }

    private String readUtf8(String bucket, String key) {
        try (InputStream input = objectStorageService.open(bucket, key)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception exception) { throw new IllegalStateException("读取文档阶段产物失败", exception); }
    }

    private void putJson(String key, Object value) {
        try { objectStorageService.put(key, objectMapper.writeValueAsBytes(value), "application/json"); }
        catch (Exception exception) { throw new IllegalStateException("保存文档阶段产物失败", exception); }
    }

    private <T> T readJson(String bucket, String key, TypeReference<T> type) {
        try (InputStream input = objectStorageService.open(bucket, key)) { return objectMapper.readValue(input, type); }
        catch (Exception exception) { throw new IllegalStateException("读取文档JSON阶段产物失败", exception); }
    }

    private KnowledgeDocumentEntity requireDocument(String id) {
        KnowledgeDocumentEntity entity = knowledgeDocumentMapper.selectById(id);
        if (entity == null) throw new IllegalStateException("知识文档不存在：" + id);
        return entity;
    }

    /** 使用数据库中的当前根任务作为fencing token，拒绝迟到的旧代际Worker。 */
    private void assertCurrentPipeline(KnowledgeDocumentEntity document,
                                       Map<String, Object> payload,
                                       String workerRootTaskId) {
        Map<String, Object> current = jdbcTemplate.queryForMap(
                "SELECT current_pipeline_root_id,pipeline_generation FROM knowledge_document WHERE id=?", document.getId());
        DocumentPipelineReliability.requireCurrentPipeline(text(current.get("current_pipeline_root_id")),
                ((Number) current.get("pipeline_generation")).longValue(), workerRootTaskId,
                integer(payload.get("pipelineGeneration"), 0));
    }

    private KnowledgeBaseEntity requireKnowledgeBase(String id) {
        KnowledgeBaseEntity entity = knowledgeBaseMapper.selectById(id);
        if (entity == null) throw new IllegalStateException("知识库不存在：" + id);
        return entity;
    }

    private Map<String, Object> payload(AsyncTaskEntity task) { return map(task.getRequestPayload()); }
    private Map<String, Object> map(String json) {
        try { return objectMapper.readValue(json == null ? "{}" : json, new TypeReference<>() { }); }
        catch (Exception ignored) { return Map.of(); }
    }
    private String json(Object value) {
        try { return objectMapper.writeValueAsString(value); }
        catch (Exception exception) { throw new IllegalStateException("文档DAG参数序列化失败", exception); }
    }
    private long scalar(String sql, Object... args) {
        Number value = jdbcTemplate.queryForObject(sql, Number.class, args);
        return value == null ? 0L : value.longValue();
    }
    private int integer(Object value, int fallback) { return value instanceof Number number ? number.intValue() : fallback; }
    private int value(Integer value) { return value == null ? 0 : value; }
    private String text(Object value) { return value == null ? "" : String.valueOf(value); }

    /**
     * 对象存储中的稳定分片描述。
     *
     * @param id 稳定分片ID
     * @param parentChunkId 父分片ID
     * @param chunkNo 文档内序号
     * @param level 分片层级
     * @param content 分片正文
     * @param sectionTitle 章节标题
     * @param sectionPath 章节路径
     * @param paragraphNo 段落序号
     * @param startOffset 起始偏移
     * @param endOffset 结束偏移
     * @param tokenCount Token估算数
     * @param contentHash 内容哈希
     * @param embeddingEnabled 是否生成向量
     */
    public record PipelineSegment(String id, String parentChunkId, Integer chunkNo, String level, String content,
                                  String sectionTitle, String sectionPath, Integer paragraphNo, Integer startOffset,
                                  Integer endOffset, Integer tokenCount, String contentHash, boolean embeddingEnabled) { }

    /**
     * 单个向量分片条目。
     *
     * @param chunkId 分片ID
     * @param vector 向量值
     */
    public record VectorItem(String chunkId, List<Double> vector) { }
}
